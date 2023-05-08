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

package app

import (
	"context"
	"fmt"
	"net/http"
	"time"

	apiextclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/healthz"

	"github.com/kubewharf/kubeadmiral/cmd/controller-manager/app/options"
	fedcorev1a1 "github.com/kubewharf/kubeadmiral/pkg/apis/core/v1alpha1"
	"github.com/kubewharf/kubeadmiral/pkg/controllermanager"
	"github.com/kubewharf/kubeadmiral/pkg/controllermanager/ftcmanager"
	"github.com/kubewharf/kubeadmiral/pkg/controllermanager/healthcheck"
	fedleaderelection "github.com/kubewharf/kubeadmiral/pkg/controllermanager/leaderelection"
	controllercontext "github.com/kubewharf/kubeadmiral/pkg/controllers/context"
)

const (
	FederatedClusterControllerName = "cluster"
	TypeConfigControllerName       = "typeconfig"
	MonitorControllerName          = "monitor"
	FollowerControllerName         = "follower"
)

var knownControllers = map[string]controllermanager.StartControllerFunc{
	FederatedClusterControllerName: startFederatedClusterController,
	TypeConfigControllerName:       startTypeConfigController,
	MonitorControllerName:          startMonitorController,
	FollowerControllerName:         startFollowerController,
}

const (
	FederateControllerName          = "federate"
	GlobalSchedulerName             = "scheduler"
	AutoMigrationControllerName     = "automigration"
	SyncControllerName              = "sync"
	StatusControllerName            = "status"
	StatusAggregationControllerName = "status-aggregation"
	PolicyRCControllerName          = "policyrc"
)

var knownFTCSubControllers = map[string]controllermanager.FTCSubControllerInitFuncs{
	GlobalSchedulerName: {
		StartFunc:     startGlobalScheduler,
		IsEnabledFunc: isGlobalSchedulerEnabled,
	},
	FederateControllerName: {
		StartFunc:     startFederateController,
		IsEnabledFunc: isFederateControllerEnabled,
	},
	AutoMigrationControllerName: {
		StartFunc:     startAutoMigrationController,
		IsEnabledFunc: isAutoMigrationControllerEnabled,
	},
	StatusControllerName: {
		StartFunc:     startStatusController,
		IsEnabledFunc: isStatusControllerEnabled,
	},
	StatusAggregationControllerName: {
		StartFunc:     startStatusAggregationController,
		IsEnabledFunc: isStatusAggregationControllerEnabled,
	},
	PolicyRCControllerName: {
		StartFunc:     startPolicyRCController,
		IsEnabledFunc: isPolicyRCControllerEnabled,
	},
	SyncControllerName: {
		StartFunc:     startSyncController,
		IsEnabledFunc: isSyncControllerEnabled,
	},
}

var controllersDisabledByDefault = sets.New(MonitorControllerName, TypeConfigControllerName)

// Run starts the controller manager according to the given options.
func Run(ctx context.Context, opts *options.Options) {
	controllerCtx, err := createControllerContext(opts)
	if err != nil {
		klog.Fatalf("Error creating controller context: %v", err)
	}

	if opts.EnableProfiling {
		go func() {
			server := &http.Server{
				Addr:              "0.0.0.0:6060",
				ReadHeaderTimeout: time.Second * 3,
			}
			if err := server.ListenAndServe(); err != nil {
				klog.Errorf("Failed to start pprof server: %v", err)
			}
		}()
	}

	healthCheckHandler := healthcheck.NewMutableHealthCheckHandler()
	healthCheckHandler.AddLivezChecker("ping", healthz.Ping)

	run := func(ctx context.Context) {
		defer klog.Infoln("Ready to stop controllers")
		klog.Infoln("Ready to start controllers")

		err := startControllers(ctx, controllerCtx, knownControllers, knownFTCSubControllers, opts.Controllers, healthCheckHandler)
		if err != nil {
			klog.Fatalf("Error starting controllers %s: %v", opts.Controllers, err)
		}

		controllerCtx.StartFactories(ctx)

		<-ctx.Done()
	}

	go func() {
		server := &http.Server{
			Addr:              fmt.Sprintf("0.0.0.0:%d", opts.Port),
			ReadHeaderTimeout: time.Second * 3,
			Handler:           healthCheckHandler,
		}
		if err := server.ListenAndServe(); err != nil {
			klog.Fatalf("Failed to start health check server: %v", err)
		}
	}()

	if opts.EnableLeaderElect {
		healthzAdaptor := leaderelection.NewLeaderHealthzAdaptor(time.Second * 20)

		elector, err := fedleaderelection.NewFederationLeaderElector(
			controllerCtx.RestConfig,
			run,
			controllerCtx.FedSystemNamespace,
			opts.LeaderElectionResourceName,
			healthzAdaptor,
		)
		if err != nil {
			klog.Fatalf("Cannot create elector: %v", err)
		}

		healthCheckHandler.AddLivezChecker("leaderElection", healthzAdaptor.Check)

		elector.Run(ctx)
	} else {
		run(ctx)
	}
}

// startControllers loops through startControllerFuncs in sequence and starts the given controller if it is enabled.
// An error is returned if one of the controller fails to start. startControllers will not block on the controllers
// and will return once they have all been successfully started.
func startControllers(
	ctx context.Context,
	controllerCtx *controllercontext.Context,
	startControllerFuncs map[string]controllermanager.StartControllerFunc,
	ftcSubControllerInitFuncs map[string]controllermanager.FTCSubControllerInitFuncs,
	enabledControllers []string,
	healthCheckHandler *healthcheck.MutableHealthCheckHandler,
) error {
	klog.Infof("Starting controllers %v", enabledControllers)

	for controllerName, initFn := range startControllerFuncs {
		if !isControllerEnabled(controllerName, controllersDisabledByDefault, enabledControllers) {
			klog.Warningf("Skipped %q, is disabled", controllerName)
			continue
		}

		controller, err := initFn(ctx, controllerCtx)
		if err != nil {
			return fmt.Errorf("error starting %q: %w", controllerName, err)
		}
		klog.Infof("Started %q", controllerName)

		healthCheckHandler.AddReadyzChecker(controllerName, func(_ *http.Request) error {
			if controller.IsControllerReady() {
				return nil
			}
			return fmt.Errorf("controller not ready")
		})
	}

	apiextclient, err := apiextclientset.NewForConfig(controllerCtx.RestConfig)
	if err != nil {
		return err
	}

	subControllerRegistry := map[string]controllermanager.FTCSubControllerInitFuncs{}
	for controllerName, initFuncs := range ftcSubControllerInitFuncs {
		controllerName := controllerName
		initFuncs := initFuncs

		subControllerRegistry[controllerName] = controllermanager.FTCSubControllerInitFuncs{
			StartFunc: initFuncs.StartFunc,
			IsEnabledFunc: func(typeConfig *fedcorev1a1.FederatedTypeConfig) bool {
				if !isControllerEnabled(controllerName, controllersDisabledByDefault, enabledControllers) {
					return false
				}
				if initFuncs.IsEnabledFunc != nil {
					return initFuncs.IsEnabledFunc(typeConfig)
				}
				return true
			},
		}
	}

	klog.Info("Starting FTC manager")
	manager := ftcmanager.NewFederatedTypeConfigManager(
		controllerCtx.ComponentConfig.FederatedTypeConfigCreateCRDsForFTCs,
		controllerCtx.KubeClientset,
		apiextclient,
		controllerCtx.FedInformerFactory.Core().V1alpha1().FederatedTypeConfigs(),
		subControllerRegistry,
		controllerCtx,
		healthCheckHandler,
		controllerCtx.Metrics,
	)
	go manager.Run(ctx)

	return nil
}
