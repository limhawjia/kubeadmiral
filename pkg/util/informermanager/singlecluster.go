package informermanager

import (
	"context"
	"fmt"
	"sync"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	fedcorev1a1 "github.com/kubewharf/kubeadmiral/pkg/apis/core/v1alpha1"
	fedcorev1a1informers "github.com/kubewharf/kubeadmiral/pkg/client/informers/externalversions/core/v1alpha1"
	schemautil "github.com/kubewharf/kubeadmiral/pkg/controllers/util/schema"
)

type singleClusterInformerManager struct {
	sync.RWMutex

	client                 dynamic.Interface
	ftcInformer            fedcorev1a1informers.FederatedTypeConfigInformer
	eventHandlerGenerators []EventHandlerGenerator

	gvkToFTCMap map[schema.GroupVersionKind]*fedcorev1a1.FederatedTypeConfig

	informers map[string]informers.GenericInformer
	stopChs   map[string]chan struct{}

	started   bool
	workqueue workqueue.Interface
	logger    klog.Logger
}

func NewSingleClusterInformerManager(
	client dynamic.Interface,
	ftcInformer fedcorev1a1informers.FederatedTypeConfigInformer,
) SingleClusterInformerManager {
	m := &singleClusterInformerManager{
		RWMutex:                sync.RWMutex{},
		client:                 client,
		ftcInformer:            ftcInformer,
		eventHandlerGenerators: []EventHandlerGenerator{},
		informers:              map[string]informers.GenericInformer{},
		stopChs:                map[string]chan struct{}{},
		workqueue:              workqueue.NewRateLimitingQueue(workqueue.DefaultItemBasedRateLimiter()),
		logger:                 klog.LoggerWithName(klog.Background(), "single-cluster-informer-manager"),
	}

	ftcInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    m.enqueue,
		DeleteFunc: m.enqueue,
	})

	return m
}

func (m *singleClusterInformerManager) AddEventHandlerGenerator(generator EventHandlerGenerator) error {
	m.Lock()
	defer m.Unlock()

	if m.started {
		return fmt.Errorf("informer manager already started")
	}

	m.eventHandlerGenerators = append(m.eventHandlerGenerators, generator)
	return nil
}

func (m *singleClusterInformerManager) GetLister(gvk schema.GroupVersionKind) (cache.GenericLister, cache.InformerSynced, bool) {
	m.RLock()
	defer m.RUnlock()

	ftc, ok := m.gvkToFTCMap[gvk]
	if !ok {
		return nil, nil, false
	}

	informer, ok := m.informers[ftc.Name]
	if !ok {
		return nil, nil, false
	}

	return informer.Lister(), informer.Informer().HasSynced, true
}

func (m *singleClusterInformerManager) GetTypeConfig(gvk schema.GroupVersionKind) (*fedcorev1a1.FederatedTypeConfig, bool) {
	m.RLock()
	defer m.RUnlock()

	ftc, ok := m.gvkToFTCMap[gvk]
	if !ok {
		return nil, false
	}

	return ftc, true
}

func (m *singleClusterInformerManager) Start(ctx context.Context) {
	m.Lock()
	defer m.Unlock()

	if m.started {
		return
	}

	m.started = true
	go wait.UntilWithContext(ctx, m.processQueueItem, 0)
	go func() {
		<-ctx.Done()
		m.shutdown()
	}()
}

func (m *singleClusterInformerManager) enqueue(obj interface{}) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		m.logger.Error(err, "Failed to enqueue FTC")
		return
	}
	m.workqueue.Add(key)
}

func (m *singleClusterInformerManager) shutdown() {
	m.Lock()
	defer m.Unlock()

	m.workqueue.ShutDown()
	for _, stopCh := range m.stopChs {
		close(stopCh)
	}
}

func (m *singleClusterInformerManager) processQueueItem(ctx context.Context) {
	key, shutdown := m.workqueue.Get()
	if shutdown {
		return
	}

	defer m.workqueue.Done(key)

	_, name, err := cache.SplitMetaNamespaceKey(key.(string))
	if err != nil {
		m.logger.Error(err, "Failed to process queue item")
	}

	if err := m.sync(name); err != nil {
		m.logger.Error(err, "Failed to process queue item")
		m.workqueue.Add(key)
	}
}

func (m *singleClusterInformerManager) sync(ftcName string) error {
	m.Lock()
	defer m.Unlock()

	ftc, err := m.ftcInformer.Lister().Get(ftcName)

	if apierrors.IsNotFound(err) {
		return m.removeFTC(ftcName)
	}

	if err != nil {
		return fmt.Errorf("failed to get FTC from store: %w", err)
	}

	return m.addFTC(ftc)
}

// NOTE: must be called only after the manager's lock is acquired by the caller
func (m *singleClusterInformerManager) removeFTC(ftcName string) error {
	stopCh, ok := m.stopChs[ftcName]
	if !ok {
		return nil
	}

	close(stopCh)
	delete(m.informers, ftcName)
	delete(m.stopChs, ftcName)

	for gvk, ftc := range m.gvkToFTCMap {
		if ftcName == ftc.Name {
			delete(m.gvkToFTCMap, gvk)
		}
	}

	return nil
}

// NOTE: must be called only after the manager's lock is acquired by the caller
func (m *singleClusterInformerManager) addFTC(ftc *fedcorev1a1.FederatedTypeConfig) error {
	if _, ok := m.informers[ftc.Name]; ok {
		return nil
	}

	gvr := schemautil.APIResourceToGVR(ftc.GetSourceType())
	gvk := schemautil.APIResourceToGVK(ftc.GetSourceType())

	stopCh := make(chan struct{})
	informer := dynamicinformer.NewFilteredDynamicInformer(
		m.client,
		gvr,
		metav1.NamespaceAll,
		0,
		cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
		nil,
	)

	for _, generator := range m.eventHandlerGenerators {
		if handler := generator(ftc); handler != nil {
			informer.Informer().AddEventHandler(handler)
		}
	}
	informer.Informer().Run(stopCh)

	m.informers[ftc.Name] = informer
	m.stopChs[ftc.Name] = stopCh

	m.gvkToFTCMap[gvk] = ftc

	return nil
}

var _ SingleClusterInformerManager = &singleClusterInformerManager{}
