package informermanager

import (
	"fmt"
	"path"
	goruntime "runtime"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/client-go/tools/cache"

	fedcorev1a1 "github.com/kubewharf/kubeadmiral/pkg/apis/core/v1alpha1"
	"github.com/onsi/gomega"
)

var (
	daemonsetFTC = &fedcorev1a1.FederatedTypeConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name: "daemonsets.apps",
		},
		Spec: fedcorev1a1.FederatedTypeConfigSpec{
			SourceType: fedcorev1a1.APIResource{
				Group:      "apps",
				Version:    "v1",
				Kind:       "DaemonSet",
				PluralName: "daemonsets",
				Scope:      v1beta1.NamespaceScoped,
			},
		},
	}
	deploymentFTC = &fedcorev1a1.FederatedTypeConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name: "deployments.apps",
		},
		Spec: fedcorev1a1.FederatedTypeConfigSpec{
			SourceType: fedcorev1a1.APIResource{
				Group:      "apps",
				Version:    "v1",
				Kind:       "Deployment",
				PluralName: "deployments",
				Scope:      v1beta1.NamespaceScoped,
			},
		},
	}
	configmapFTC = &fedcorev1a1.FederatedTypeConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name: "configmaps",
		},
		Spec: fedcorev1a1.FederatedTypeConfigSpec{
			SourceType: fedcorev1a1.APIResource{
				Group:      "",
				Version:    "v1",
				Kind:       "ConfigMap",
				PluralName: "configmaps",
				Scope:      v1beta1.NamespaceScoped,
			},
		},
	}
	secretFTC = &fedcorev1a1.FederatedTypeConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name: "secrets",
		},
		Spec: fedcorev1a1.FederatedTypeConfigSpec{
			SourceType: fedcorev1a1.APIResource{
				Group:      "",
				Version:    "v1",
				Kind:       "Secret",
				PluralName: "secrets",
				Scope:      v1beta1.NamespaceScoped,
			},
		},
	}
)

var (
	deploymentGVK = appsv1.SchemeGroupVersion.WithKind("Deployment")
	daemonsetGVK  = appsv1.SchemeGroupVersion.WithKind("DaemonSet")
	configmapGVK  = corev1.SchemeGroupVersion.WithKind("ConfigMap")
	secretGVK     = corev1.SchemeGroupVersion.WithKind("Secret")
)

func getTestDeployment(name, namespace string) *unstructured.Unstructured {
	dp := &appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Deployment",
			APIVersion: "apps/v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
	}

	dpMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(dp)
	if err != nil {
		panic(err)
	}

	return &unstructured.Unstructured{Object: dpMap}
}

func getTestConfigMap(name, namespace string) *unstructured.Unstructured {
	cm := &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
	}

	cmMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(cm)
	if err != nil {
		panic(err)
	}

	return &unstructured.Unstructured{Object: cmMap}
}

func getTestSecret(name, namespace string) *unstructured.Unstructured {
	secret := &corev1.Secret{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Secret",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
	}

	secretMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(secret)
	if err != nil {
		panic(err)
	}

	return &unstructured.Unstructured{Object: secretMap}
}

func getTestDaemonSet(name, namespace string) *unstructured.Unstructured {
	dm := &appsv1.DaemonSet{
		TypeMeta: metav1.TypeMeta{
			Kind:       "DaemonSet",
			APIVersion: "apps/v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
	}

	dmMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(dm)
	if err != nil {
		panic(err)
	}

	return &unstructured.Unstructured{Object: dmMap}
}

func getTestCluster(name string) *fedcorev1a1.FederatedCluster {
	return &fedcorev1a1.FederatedCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: fedcorev1a1.FederatedClusterSpec{
			APIEndpoint:            rand.String(24),
			Insecure:               false,
			UseServiceAccountToken: true,
			SecretRef: fedcorev1a1.LocalSecretReference{
				Name: name,
			},
			Taints: []corev1.Taint{},
		},
		Status: fedcorev1a1.FederatedClusterStatus{
			Conditions: []fedcorev1a1.ClusterCondition{
				{
					Type:               fedcorev1a1.ClusterJoined,
					Status:             corev1.ConditionTrue,
					LastProbeTime:      metav1.Now(),
					LastTransitionTime: metav1.Now(),
				},
			},
			JoinPerformed: true,
		},
	}
}

func newCountingResourceEventHandler() *countingResourceEventHandler {
	return &countingResourceEventHandler{
		lock:                     sync.RWMutex{},
		generateCount:            map[string]int{},
		addEventCount:            map[schema.GroupVersionKind]int{},
		updateEventCount:         map[schema.GroupVersionKind]int{},
		deleteEventCount:         map[schema.GroupVersionKind]int{},
		expectedGenerateCount:    map[string]int{},
		expectedAddEventCount:    map[schema.GroupVersionKind]int{},
		expectedUpdateEventCount: map[schema.GroupVersionKind]int{},
		expectedDeleteEventCount: map[schema.GroupVersionKind]int{},
	}
}

type countingResourceEventHandler struct {
	lock sync.RWMutex

	generateCount    map[string]int
	addEventCount    map[schema.GroupVersionKind]int
	updateEventCount map[schema.GroupVersionKind]int
	deleteEventCount map[schema.GroupVersionKind]int

	expectedGenerateCount    map[string]int
	expectedAddEventCount    map[schema.GroupVersionKind]int
	expectedUpdateEventCount map[schema.GroupVersionKind]int
	expectedDeleteEventCount map[schema.GroupVersionKind]int
}

func (h *countingResourceEventHandler) ExpectGenerateEvents(ftcName string, n int) {
	h.lock.Lock()
	defer h.lock.Unlock()
	h.expectedGenerateCount[ftcName] = h.expectedGenerateCount[ftcName] + n
}

func (h *countingResourceEventHandler) ExpectAddEvents(gvk schema.GroupVersionKind, n int) {
	h.lock.Lock()
	defer h.lock.Unlock()
	h.expectedAddEventCount[gvk] = h.expectedAddEventCount[gvk] + n
}

func (h *countingResourceEventHandler) ExpectUpdateEvents(gvk schema.GroupVersionKind, n int) {
	h.lock.Lock()
	defer h.lock.Unlock()
	h.expectedUpdateEventCount[gvk] = h.expectedUpdateEventCount[gvk] + n
}

func (h *countingResourceEventHandler) ExpectDeleteEvents(gvk schema.GroupVersionKind, n int) {
	h.lock.Lock()
	defer h.lock.Unlock()
	h.expectedDeleteEventCount[gvk] = h.expectedDeleteEventCount[gvk] + n
}

func (h *countingResourceEventHandler) AssertEventually(g gomega.Gomega, timeout time.Duration) {
	_, file, no, _ := goruntime.Caller(1)
	callerInfo := fmt.Sprintf("%s:%d", path.Base(file), no)

	g.Eventually(func(g gomega.Gomega) {
		for ftc := range h.expectedGenerateCount {
			g.Expect(h.generateCount[ftc]).
				To(gomega.BeNumerically("==", h.expectedGenerateCount[ftc]), "%s: incorrect number of generate events for %s", callerInfo, ftc)
		}
		for gvk := range h.expectedAddEventCount {
			g.Expect(h.addEventCount[gvk]).
				To(gomega.BeNumerically("==", h.expectedAddEventCount[gvk]), "%s: incorrect number of add events for %s", callerInfo, gvk)
		}
		for gvk := range h.expectedUpdateEventCount {
			g.Expect(h.updateEventCount[gvk]).
				To(gomega.BeNumerically("==", h.expectedUpdateEventCount[gvk]), "%s: incorrect number of update events for %s", callerInfo, gvk)
		}
		for gvk := range h.expectedDeleteEventCount {
			g.Expect(h.deleteEventCount[gvk]).
				To(gomega.BeNumerically("==", h.expectedDeleteEventCount[gvk]), "%s: incorrect number of delete events for %s", callerInfo, gvk)
		}
	}).WithTimeout(timeout).Should(gomega.Succeed())
}

func (h *countingResourceEventHandler) AssertConsistently(g gomega.Gomega, timeout time.Duration) {
	_, file, no, _ := goruntime.Caller(1)
	callerInfo := fmt.Sprintf("%s:%d", file, no)

	g.Consistently(func(g gomega.Gomega) {
		for ftc := range h.expectedGenerateCount {
			g.Expect(h.generateCount[ftc]).
				To(gomega.BeNumerically("==", h.expectedGenerateCount[ftc]), "%s: incorrect number of generate events for %s", callerInfo, ftc)
		}
		for gvk := range h.expectedAddEventCount {
			g.Expect(h.addEventCount[gvk]).
				To(gomega.BeNumerically("==", h.expectedAddEventCount[gvk]), "%s: incorrect number of add events for %s", callerInfo, gvk)
		}
		for gvk := range h.expectedUpdateEventCount {
			g.Expect(h.updateEventCount[gvk]).
				To(gomega.BeNumerically("==", h.expectedUpdateEventCount[gvk]), "%s: incorrect number of update events for %s", callerInfo, gvk)
		}
		for gvk := range h.expectedDeleteEventCount {
			g.Expect(h.deleteEventCount[gvk]).
				To(gomega.BeNumerically("==", h.expectedDeleteEventCount[gvk]), "%s: incorrect number of delete events for %s", callerInfo, gvk)
		}
	}).WithTimeout(timeout).Should(gomega.Succeed())
}

func (h *countingResourceEventHandler) GenerateEventHandler(ftc *fedcorev1a1.FederatedTypeConfig) cache.ResourceEventHandler {
	h.lock.Lock()
	defer h.lock.Unlock()
	h.generateCount[ftc.Name] = h.generateCount[ftc.Name] + 1
	return h
}

func (h *countingResourceEventHandler) OnAdd(obj interface{}) {
	h.lock.Lock()
	defer h.lock.Unlock()

	gvk := h.mustParseObject(obj)
	h.addEventCount[gvk] = h.addEventCount[gvk] + 1
}

func (h *countingResourceEventHandler) OnDelete(obj interface{}) {
	h.lock.Lock()
	defer h.lock.Unlock()

	gvk := h.mustParseObject(obj)
	h.deleteEventCount[gvk] = h.deleteEventCount[gvk] + 1
}

func (h *countingResourceEventHandler) OnUpdate(_ interface{}, obj interface{}) {
	h.lock.Lock()
	defer h.lock.Unlock()

	gvk := h.mustParseObject(obj)
	h.updateEventCount[gvk] = h.updateEventCount[gvk] + 1
}

func (h *countingResourceEventHandler) mustParseObject(obj interface{}) schema.GroupVersionKind {
	uns := obj.(*unstructured.Unstructured)
	gv, err := schema.ParseGroupVersion(uns.GetAPIVersion())
	if err != nil {
		panic(fmt.Errorf("failed to parse GroupVersion from unstructured: %w", err))
	}
	return gv.WithKind(uns.GetKind())
}

var _ cache.ResourceEventHandler = &countingResourceEventHandler{}

func alwaysRegisterPredicate(_, _ *fedcorev1a1.FederatedTypeConfig) bool {
	return true
}

func neverRegisterPredicate(_, _ *fedcorev1a1.FederatedTypeConfig) bool {
	return false
}

func registerOncePredicate(old, _ *fedcorev1a1.FederatedTypeConfig) bool {
	return old == nil
}

func newAnnotationBasedGenerator(handler *countingResourceEventHandler) *EventHandlerGenerator {
	return &EventHandlerGenerator{
		Predicate: func(_, latest *fedcorev1a1.FederatedTypeConfig) bool {
			return latest.GetAnnotations()["predicate"] == "true"
		},
		Generator: func(ftc *fedcorev1a1.FederatedTypeConfig) cache.ResourceEventHandler {
			if ftc.GetAnnotations()["generator"] == "true" {
				return handler.GenerateEventHandler(ftc)
			}
			return nil
		},
	}
}
