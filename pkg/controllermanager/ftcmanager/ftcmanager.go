/*
Copyright 2023 The KubeAdmiral Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package ftcmanager

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	apiextclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	typedapiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/typed/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	fedcorev1a1informers "github.com/kubewharf/kubeadmiral/pkg/client/informers/externalversions/core/v1alpha1"
	"github.com/kubewharf/kubeadmiral/pkg/controllermanager"
	"github.com/kubewharf/kubeadmiral/pkg/controllermanager/healthcheck"
	"github.com/kubewharf/kubeadmiral/pkg/controllers/common"
	controllercontext "github.com/kubewharf/kubeadmiral/pkg/controllers/context"
	"github.com/kubewharf/kubeadmiral/pkg/controllers/util"
	"github.com/kubewharf/kubeadmiral/pkg/controllers/util/delayingdeliver"
	"github.com/kubewharf/kubeadmiral/pkg/controllers/util/worker"
	"github.com/kubewharf/kubeadmiral/pkg/stats"
)

type FederatedTypeConfigManager struct {
	createCRDsForFTCs bool
	discoveryClient   discovery.DiscoveryInterface
	crdClient         typedapiextensionsv1.CustomResourceDefinitionInterface

	informer fedcorev1a1informers.FederatedTypeConfigInformer
	registry map[string]controllermanager.FTCSubControllerInitFuncs

	lock sync.Mutex
	// cancelFuncs is a map containing the cancel funcs for each started subcontroller
	cancelFuncs map[string]context.CancelFunc

	controllerCtx      *controllercontext.Context
	healthCheckHandler *healthcheck.MutableHealthCheckHandler

	worker worker.ReconcileWorker

	metrics stats.Metrics
	logger  klog.Logger
}

func NewFederatedTypeConfigManager(
	createCRDsForFTCs bool,
	kubeClient kubernetes.Interface,
	apiextClient apiextclientset.Interface,
	informer fedcorev1a1informers.FederatedTypeConfigInformer,
	knownSubControllers map[string]controllermanager.FTCSubControllerInitFuncs,
	controllerCtx *controllercontext.Context,
	healthCheckHandler *healthcheck.MutableHealthCheckHandler,
	metrics stats.Metrics,
) *FederatedTypeConfigManager {
	m := &FederatedTypeConfigManager{
		createCRDsForFTCs:  createCRDsForFTCs,
		discoveryClient:    kubeClient.Discovery(),
		crdClient:          apiextClient.ApiextensionsV1().CustomResourceDefinitions(),
		informer:           informer,
		registry:           knownSubControllers,
		lock:               sync.Mutex{},
		cancelFuncs:        map[string]context.CancelFunc{},
		controllerCtx:      controllerCtx,
		healthCheckHandler: healthCheckHandler,
		metrics:            metrics,
		logger:             klog.LoggerWithValues(klog.Background(), "controller", "federated-type-config-manager"),
	}

	m.worker = worker.NewReconcileWorker(
		m.reconcile,
		worker.WorkerTiming{},
		1,
		metrics,
		delayingdeliver.NewMetricTags("federated-type-config-manager", "FederatedTypeConfig"),
	)

	informer.Informer().AddEventHandler(util.NewTriggerOnAllChanges(m.worker.EnqueueObject))

	return m
}

func (m *FederatedTypeConfigManager) Run(ctx context.Context) {
	m.logger.Info("Starting FederatedTypeConfig manager")
	defer m.logger.Info("Stopping FederatedTypeConfig manager")

	if !cache.WaitForNamedCacheSync("federated-type-config-manager", ctx.Done(), m.informer.Informer().HasSynced) {
		return
	}

	m.worker.Run(ctx.Done())
	<-ctx.Done()
}

func (m *FederatedTypeConfigManager) reconcile(qualifiedName common.QualifiedName) (status worker.Result) {
	_ = m.metrics.Rate("federated-type-config-manager.throughput", 1)
	key := qualifiedName.String()
	logger := m.logger.WithValues("federated-type-config", key)
	startTime := time.Now()
	ctx := klog.NewContext(context.TODO(), logger)

	logger.V(3).Info("Start reconcile")
	defer m.metrics.Duration("federated-type-config-manager.latency", startTime)
	defer func() {
		logger.WithValues("duration", time.Since(startTime), "status", status.String()).V(3).Info("Finished reconcile")
	}()

	typeConfig, err := m.informer.Lister().Get(qualifiedName.Name)
	if err != nil && apierrors.IsNotFound(err) {
		logger.V(3).Info("Observed FederatedTypeConfig deletion")
		m.processFTCDeletion(ctx, qualifiedName.Name)
		return worker.StatusAllOK
	}
	if err != nil {
		logger.Error(err, "Failed to get FederatedTypeConfig")
		return worker.StatusError
	}

	if m.createCRDsForFTCs {
		if err := m.ensureFederatedObjectCRD(ctx, typeConfig); err != nil {
			logger.Error(err, "Failed to ensure federated object CRD")
			return worker.StatusError
		}
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	needRetry := false
	for controllerName, initFuncs := range m.registry {
		logger := logger.WithValues("subcontroller", controllerName)

		resolvedName := resolveSubcontrollerName(controllerName, typeConfig.Name)
		cancelFunc, controllerStarted := m.cancelFuncs[resolvedName]

		// registered subcontrollers are enabled by default
		controllerEnabled := true
		if initFuncs.IsEnabledFunc != nil {
			controllerEnabled = initFuncs.IsEnabledFunc(typeConfig)
		}

		if controllerEnabled && !controllerStarted {
			ctx, cancel := context.WithCancel(context.Background())
			controller, err := initFuncs.StartFunc(ctx, m.controllerCtx, typeConfig)
			if err != nil {
				logger.Error(err, "Failed to start subcontroller")
				needRetry = true
				cancel()
				continue
			}

			logger.Info("Started subcontroller")
			m.cancelFuncs[resolvedName] = cancel
			m.healthCheckHandler.AddReadyzChecker(resolvedName, func(req *http.Request) error {
				if controller.IsControllerReady() {
					return nil
				}
				return fmt.Errorf("controller not ready")
			})
		}

		if !controllerEnabled && controllerStarted {
			cancelFunc()
			delete(m.cancelFuncs, resolvedName)
			m.healthCheckHandler.RemoveReadyzChecker(resolvedName)

			logger.Info("Stopped subcontroller")
		}
	}

	// Since the controllers are created dynamically, we have to start the informer factories again, in case any new
	// informers were accessed. Note that a different context is used in case a FTC is recreated and the same informer
	// needs to be used again (SharedInformerFactory and SharedInformers do not support restarts).
	ctx = context.TODO()
	m.controllerCtx.KubeInformerFactory.Start(ctx.Done())
	m.controllerCtx.DynamicInformerFactory.Start(ctx.Done())
	m.controllerCtx.FedInformerFactory.Start(ctx.Done())

	if needRetry {
		return worker.StatusError
	}

	return worker.StatusAllOK
}

func (m *FederatedTypeConfigManager) processFTCDeletion(ctx context.Context, ftcName string) {
	logger := klog.FromContext(ctx)

	m.lock.Lock()
	defer m.lock.Unlock()

	for controllerName := range m.registry {
		logger := logger.WithValues("subcontroller", controllerName)

		resolvedName := resolveSubcontrollerName(controllerName, ftcName)
		cancel, started := m.cancelFuncs[resolvedName]
		if !started {
			continue
		}

		cancel()
		delete(m.cancelFuncs, resolvedName)
		m.healthCheckHandler.RemoveReadyzChecker(resolvedName)

		logger.Info("Stopped subcontroller")
	}
}

func resolveSubcontrollerName(baseName, ftcName string) string {
	return fmt.Sprintf("%s[%s]", ftcName, baseName)
}
