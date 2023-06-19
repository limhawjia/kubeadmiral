package informermanager

import (
	"context"
	"fmt"
	"sync"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	"github.com/kubewharf/kubeadmiral/pkg/apis/core/v1alpha1"
	fedcorev1a1 "github.com/kubewharf/kubeadmiral/pkg/apis/core/v1alpha1"
	fedcorev1a1informers "github.com/kubewharf/kubeadmiral/pkg/client/informers/externalversions/core/v1alpha1"
	"github.com/kubewharf/kubeadmiral/pkg/controllers/util"
)

type multiClusterInformerManager struct {
	sync.RWMutex

	fedSystemNamespace     string
	kubeClient             kubernetes.Interface
	dynamicClient          dynamic.Interface
	ftcInformer            fedcorev1a1informers.FederatedTypeConfigInformer
	clusterInformer        fedcorev1a1informers.FederatedClusterInformer
	eventHandlerGenerators []EventHandlerGenerator

	managers    map[string]SingleClusterInformerManager
	cancelFuncs map[string]context.CancelFunc

	started   bool
	workqueue workqueue.Interface
	logger    klog.Logger
}

func NewMultiClusterInformerManager(
	fedSystemNamespace string,
	kubeClient kubernetes.Interface,
	dynamicClient dynamic.Interface,
	ftcInformer fedcorev1a1informers.FederatedTypeConfigInformer,
	clusterInformer fedcorev1a1informers.FederatedClusterInformer,
) MultiClusterInformerManager {
	m := &multiClusterInformerManager{
		RWMutex:                sync.RWMutex{},
		fedSystemNamespace:     fedSystemNamespace,
		kubeClient:             kubeClient,
		dynamicClient:          dynamicClient,
		ftcInformer:            ftcInformer,
		clusterInformer:        clusterInformer,
		eventHandlerGenerators: []EventHandlerGenerator{},
		managers:               map[string]SingleClusterInformerManager{},
		cancelFuncs:            map[string]context.CancelFunc{},
		started:                false,
		workqueue:              nil,
		logger:                 klog.LoggerWithName(klog.Background(), "multi-cluster-informer-manager"),
	}

	clusterInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: func(obj interface{}) bool {
			cluster := obj.(*fedcorev1a1.FederatedCluster)
			return util.IsClusterJoined(&cluster.Status)
		},
		Handler: cache.ResourceEventHandlerFuncs{
			AddFunc:    m.enqueue,
			DeleteFunc: m.enqueue,
		},
	})

	return m
}

func (m *multiClusterInformerManager) AddEventHandlerGenerator(generator EventHandlerGenerator) error {
	m.Lock()
	defer m.Unlock()

	if m.started {
		return fmt.Errorf("informer manager already started")
	}

	m.eventHandlerGenerators = append(m.eventHandlerGenerators, generator)
	return nil
}

func (m *multiClusterInformerManager) GetListerForCluster(
	gvk schema.GroupVersionKind,
	cluster string,
) (cache.GenericLister, cache.InformerSynced, bool) {
	m.RLock()
	defer m.RUnlock()

	manager, ok := m.managers[cluster]
	if !ok {
		return nil, nil, false
	}

	return manager.GetLister(gvk)
}

func (m *multiClusterInformerManager) GetTypeConfig(gvk schema.GroupVersionKind) (*v1alpha1.FederatedTypeConfig, bool) {
	m.RLock()
	defer m.RUnlock()

	for _, manager := range m.managers {
		if ftc, ok := manager.GetTypeConfig(gvk); ok {
			return ftc, true
		}
	}

	return nil, false
}

func (m *multiClusterInformerManager) Start(ctx context.Context) {
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

func (m *multiClusterInformerManager) enqueue(obj interface{}) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		m.logger.Error(err, "Failed to enqueue FTC")
		return
	}
	m.workqueue.Add(key)
}

func (m *multiClusterInformerManager) shutdown() {
	m.Lock()
	defer m.Unlock()

	m.workqueue.ShutDown()
	for _, cancelFunc := range m.cancelFuncs {
		cancelFunc()
	}
}

func (m *multiClusterInformerManager) processQueueItem(ctx context.Context) {
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

func (m *multiClusterInformerManager) sync(clusterName string) error {
	m.Lock()
	defer m.Unlock()

	cluster, err := m.clusterInformer.Lister().Get(clusterName)

	if apierrors.IsNotFound(err) {
		return m.removeCluster(clusterName)
	}

	if err != nil {
		return fmt.Errorf("failed to get FTC from store: %w", err)
	}

	return m.addCluster(cluster)
}

// NOTE: must be called only after the manager's lock is acquired by the caller
func (m *multiClusterInformerManager) removeCluster(clusterName string) error {
	cancelFunc, ok := m.cancelFuncs[clusterName]
	if !ok {
		return nil
	}

	cancelFunc()
	delete(m.managers, clusterName)
	delete(m.cancelFuncs, clusterName)

	return nil
}

// NOTE: must be called only after the manager's lock is acquired by the caller
func (m *multiClusterInformerManager) addCluster(cluster *fedcorev1a1.FederatedCluster) error {
	if _, ok := m.managers[cluster.Name]; ok {
		return nil
	}

	config, err := util.BuildClusterConfig(cluster, m.kubeClient, &rest.Config{}, m.fedSystemNamespace)
	if err != nil {
		return fmt.Errorf("failed to build cluster config: %w", err)
	}
	client, err := dynamic.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("failed to build cluster client: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	manager := NewSingleClusterInformerManager(client, m.ftcInformer)
	manager.Start(ctx)

	m.managers[cluster.Name] = manager
	m.cancelFuncs[cluster.Name] = cancel
	return nil
}

var _ MultiClusterInformerManager = &multiClusterInformerManager{}
