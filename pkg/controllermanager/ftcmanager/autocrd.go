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
	"bytes"
	"context"
	"fmt"
	"strings"

	fedcorev1a1 "github.com/kubewharf/kubeadmiral/pkg/apis/core/v1alpha1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/klog/v2"
)

const fedObjectSchemaYaml = `
openAPIV3Schema:
	type: object
	properties:
		apiVersion:
			type: string
		kind:
			type: string
		metadata:
			type: object
		spec:
			type: object
			x-kubernetes-preserve-unknown-fields: true
		status:
			type: object
			properties:
				clusters:
					type: array
					items:
						type: object
						properties:
							generation:
								type: integer
							name:
								type: string
							status:
								type: string
						required: [name]
				conditions:
					type: array
					items:
						type: object
						properties:
							lastTransitionTime:
								type: string
								format: date-time
							lastUpdateTime:
								type: string
								format: date-time
							reason:
								type: string
							status:
								type: string
							type:
								type: string
						required: [type, status]
	required: [spec]
	x-kubernetes-preserve-unknown-fields: true
`
const statusObjectSchemaYaml = `
openAPIV3Schema:
	type: object
	x-kubernetes-preserve-unknown-fields: true
`

var fedObjectSchema, statusObjectSchema apiextensionsv1.CustomResourceValidation

func init() {
	if err := yaml.NewYAMLOrJSONDecoder(
		bytes.NewReader([]byte(strings.Replace(fedObjectSchemaYaml, "\t", "  ", -1))),
		len(fedObjectSchemaYaml),
	).Decode(&fedObjectSchema); err != nil {
		panic(err)
	}

	if err := yaml.NewYAMLOrJSONDecoder(
		bytes.NewReader([]byte(strings.Replace(statusObjectSchemaYaml, "\t", "  ", -1))),
		len(statusObjectSchemaYaml),
	).Decode(&statusObjectSchema); err != nil {
		panic(err)
	}
}

func (m *FederatedTypeConfigManager) ensureFederatedObjectCRD(ctx context.Context, typeConfig *fedcorev1a1.FederatedTypeConfig) error {
	// 1. check if federated object crd needs to be created
	fedType := typeConfig.Spec.FederatedType
	crdName := fedType.PluralName
	if fedType.Group != "" {
		crdName += "."
		crdName += fedType.Group
	}
	_, err := m.crdClient.Get(context.TODO(), crdName, metav1.GetOptions{ResourceVersion: "0"})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("cannot check for existence of CRD %q: %w", crdName, err)
	}
	needObjectCrd := err != nil

	// 2. check if federated object status crd needs to be created
	needStatusCrd := false
	statusType := typeConfig.Spec.StatusType
	var statusCrdName string
	if statusType != nil {
		statusCrdName = statusType.PluralName
		if statusType.Group != "" {
			statusCrdName += "."
			statusCrdName += statusType.Group
		}
		_, err = m.crdClient.Get(context.TODO(), statusCrdName, metav1.GetOptions{ResourceVersion: "0"})
		if err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("cannot check for existence of CRD %q: %w", statusCrdName, err)
		}
		needStatusCrd = err != nil
	}

	// early return if no crd needs to be created
	if !needObjectCrd && !needStatusCrd {
		return nil
	}

	// 3. get the source type
	var sourceResource *metav1.APIResource
	if typeConfig.Spec.SourceType != nil {
		srcType := typeConfig.Spec.SourceType
		resourceList, err := m.discoveryClient.ServerResourcesForGroupVersion(schema.GroupVersion{
			Group:   srcType.Group,
			Version: srcType.Version,
		}.String())
		if err != nil {
			return fmt.Errorf("cannot invoke discovery client: %w", err)
		}
		for _, resource := range resourceList.APIResources {
			// we don't care about resource.Group because subresources are not supported
			if resource.Name == srcType.PluralName {
				resource := resource
				sourceResource = &resource
				break
			}
		}
	}

	// 4. create federated object CRD if needed
	logger := klog.FromContext(ctx)
	if needObjectCrd {
		logger.V(1).Info("Creating federated CRD for FTC")

		fedShortNames := []string{"f" + strings.ToLower(typeConfig.Spec.TargetType.Kind)}
		if sourceResource != nil {
			for _, shortName := range sourceResource.ShortNames {
				fedShortNames = append(fedShortNames, "f"+shortName)
			}
		}

		crd := &apiextensionsv1.CustomResourceDefinition{
			ObjectMeta: metav1.ObjectMeta{
				Name: crdName,
			},
			Spec: apiextensionsv1.CustomResourceDefinitionSpec{
				Group: fedType.Group,
				Names: apiextensionsv1.CustomResourceDefinitionNames{
					Plural:     fedType.PluralName,
					Kind:       fedType.Kind,
					Singular:   strings.ToLower(fedType.Kind),
					ShortNames: fedShortNames,
					ListKind:   fedType.Kind + "List",
				},
				Scope: apiextensionsv1.ResourceScope(fedType.Scope),
				Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
					{
						Name:    fedType.Version,
						Served:  true,
						Storage: true,
						Subresources: &apiextensionsv1.CustomResourceSubresources{
							Status: &apiextensionsv1.CustomResourceSubresourceStatus{},
						},
						Schema: &fedObjectSchema,
					},
				},
			},
		}
		_, err = m.crdClient.Create(context.TODO(), crd, metav1.CreateOptions{})
		if err != nil {
			return err
		}
	}

	// 5. create federated object status CRD if needed
	if needStatusCrd {
		logger.V(1).Info("Created federated object status CRD for FTC")

		statusShortNames := []string{fmt.Sprintf("f%sstatus", strings.ToLower(typeConfig.Spec.TargetType.Kind))}
		if sourceResource != nil {
			for _, shortName := range sourceResource.ShortNames {
				statusShortNames = append(statusShortNames, fmt.Sprintf("f%sstatus", shortName))
			}
		}

		crd := &apiextensionsv1.CustomResourceDefinition{
			ObjectMeta: metav1.ObjectMeta{
				Name: statusCrdName,
			},
			Spec: apiextensionsv1.CustomResourceDefinitionSpec{
				Group: statusType.Group,
				Names: apiextensionsv1.CustomResourceDefinitionNames{
					Plural:     statusType.PluralName,
					Kind:       statusType.Kind,
					Singular:   strings.ToLower(statusType.Kind),
					ShortNames: statusShortNames,
					ListKind:   statusType.Kind + "List",
				},
				Scope: apiextensionsv1.ResourceScope(statusType.Scope),
				Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
					{
						Name:    statusType.Version,
						Served:  true,
						Storage: true,
						Subresources: &apiextensionsv1.CustomResourceSubresources{
							Status: &apiextensionsv1.CustomResourceSubresourceStatus{},
						},
						Schema: &statusObjectSchema,
					},
				},
			},
		}
		_, err = m.crdClient.Create(context.TODO(), crd, metav1.CreateOptions{})
		if err != nil {
			return err
		}
	}

	return nil
}
