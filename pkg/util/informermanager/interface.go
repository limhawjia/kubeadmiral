package informermanager

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/cache"

	fedcorev1a1 "github.com/kubewharf/kubeadmiral/pkg/apis/core/v1alpha1"
)

type EventHandlerGenerator func(*fedcorev1a1.FederatedTypeConfig) cache.ResourceEventHandler

type SingleClusterInformerManager interface {
	AddEventHandlerGenerator(generator EventHandlerGenerator) error

	GetLister(gvk schema.GroupVersionKind) (cache.GenericLister, cache.InformerSynced, bool)
	GetTypeConfig(gvk schema.GroupVersionKind) (*fedcorev1a1.FederatedTypeConfig, bool)

	Start(ctx context.Context)
}

type MultiClusterInformerManager interface {
	AddEventHandlerGenerator(generator EventHandlerGenerator) error

	GetListerForCluster(gvk schema.GroupVersionKind, cluster string) (cache.GenericLister, cache.InformerSynced, bool)
	GetTypeConfig(gvk schema.GroupVersionKind) (*fedcorev1a1.FederatedTypeConfig, bool)

	Start(ctx context.Context)
}
