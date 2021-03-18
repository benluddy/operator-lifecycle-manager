package finalizers

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsv1listers "k8s.io/apiextensions-apiserver/pkg/client/listers/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"

	v1 "github.com/operator-framework/api/pkg/operators/v1"
	"github.com/operator-framework/api/pkg/operators/v1alpha1"
	operatorsv1alpha1client "github.com/operator-framework/operator-lifecycle-manager/pkg/api/client/clientset/versioned/typed/operators/v1alpha1"
	"github.com/operator-framework/operator-lifecycle-manager/pkg/controller/registry/resolver"
)

const (
	FinalizerDeleteCustomResources = "operatorframework.io/delete-custom-resources"
)

type NeedsRequeue struct {
	delay   time.Duration
	wrapped error
}

func (e NeedsRequeue) Delay() time.Duration {
	return e.delay
}

func (e NeedsRequeue) Error() string {
	if e.wrapped != nil {
		return fmt.Sprintf("needs requeue after %s: %s", e.delay, e.wrapped.Error())
	}
	return fmt.Sprintf("needs requeue after %s", e.delay)
}

func (e NeedsRequeue) Unwrap() error {
	return e.wrapped
}

type OperandDeleterDependencies struct {
	Logger                         logrus.FieldLogger
	DynamicClient                  dynamic.Interface
	OperatorsV1Alpha1Client        operatorsv1alpha1client.OperatorsV1alpha1Interface
	CustomResourceDefinitionLister apiextensionsv1listers.CustomResourceDefinitionLister
}

func NewOperandDeleter(deps *OperandDeleterDependencies) *OperandDeleter {
	return &OperandDeleter{
		logger: deps.Logger,
		finalizerRemover: &finalizerRemoverImpl{
			client: deps.OperatorsV1Alpha1Client,
		},
		deletableResourceDefinitionGetter: &deletableResourceDefinitionGetterImpl{
			crdGetter: deps.CustomResourceDefinitionLister,
		},
		resourceDeleter: &resourceDeleterImpl{
			client: deps.DynamicClient,
		},
	}
}

type OperandDeleter struct {
	logger                            logrus.FieldLogger
	finalizerRemover                  finalizerRemover
	deletableResourceDefinitionGetter deletableResourceDefinitionGetter
	resourceDeleter                   resourceDeleter
}

// DeleteOperands performs deletion of custom resources owned by a
// given ClusterServiceVersion. Returns true if and only if the
// provided CleanupStatus was updated. A non-nil error is returned on
// failure to completely perform deletion.
func (d *OperandDeleter) DeleteOperands(csv *v1alpha1.ClusterServiceVersion, status *v1alpha1.CleanupStatus) (statusUpdated bool, err error) {
	if csv.ObjectMeta.DeletionTimestamp.IsZero() {
		return false, nil
	}

	finalizerPresent := false
	for _, finalizer := range csv.Finalizers {
		if finalizer == FinalizerDeleteCustomResources {
			finalizerPresent = true
			break
		}
	}
	if !finalizerPresent {
		return false, nil
	}

	defer func() {
		if err != nil {
			return
		}
		err = d.finalizerRemover.RemoveFinalizer(csv)
	}()

	if !csv.Spec.Cleanup.Enabled {
		d.logger.Info("operand cleanup is disabled by the csv spec")
		return false, nil
	}

	if csv.Status.Phase != v1alpha1.CSVPhaseSucceeded {
		d.logger.Info("operand cleanup cancelled because csv is in phase %s", csv.Status.Phase)
		return false, nil
	}

	crds, err := d.deletableResourceDefinitionGetter.GetDeletableResourceDefinitions(csv)
	if err != nil {
		return false, fmt.Errorf("error computing set of deletable custom resource types: %w", err)
	}

	var targets resolver.NamespaceSet
	if anno, ok := csv.Annotations[v1.OperatorGroupTargetsAnnotationKey]; ok {
		targets = resolver.NewNamespaceSetFromString(anno)
	}

	// todo: reuse resource lists from previous status
	status.PendingDeletion = nil
	for _, crd := range crds {
		instances, err := d.resourceDeleter.DeleteResources(crd, targets)
		if len(instances) > 0 {
			status.PendingDeletion = append(status.PendingDeletion, v1alpha1.ResourceList{
				Group:     crd.Spec.Group,
				Kind:      crd.Spec.Names.Kind,
				Instances: instances,
			})
		}
		if err != nil {
			return true, fmt.Errorf("error deleting instances of crd %q: %w", crd.Name, err)
		}
	}

	if len(status.PendingDeletion) > 0 {
		return true, NeedsRequeue{delay: 15 * time.Second}
	}

	return true, nil
}

type resourceDeleter interface {
	DeleteResources(crd *apiextensionsv1.CustomResourceDefinition, namespaces resolver.NamespaceSet) ([]v1alpha1.ResourceInstance, error)
}

type resourceDeleterImpl struct {
	client dynamic.Interface
}

func (d *resourceDeleterImpl) DeleteResources(crd *apiextensionsv1.CustomResourceDefinition, namespaces resolver.NamespaceSet) ([]v1alpha1.ResourceInstance, error) {
	var version string
	for _, each := range crd.Spec.Versions {
		if !each.Served {
			continue
		}
		version = each.Name
	}
	if version == "" {
		return nil, nil
	}

	crc := d.client.Resource(schema.GroupResource{
		Resource: crd.Spec.Names.Plural,
		Group:    crd.Spec.Group,
	}.WithVersion(version))

	crlist, err := crc.List(context.TODO(), metav1.ListOptions{
		// how to fetch crs in target namespaces on clusters with a large number of namespaces?
		// - 1 list per namespace -- can't scale with #ns, 10k+ ns clusters are real
		// - get -A (with Limit) probably fine if target is AllNamespaces
		// - what is the break-even point?
		Limit: 200, // may return no crs in targeted namespaces at all, even if they exist! :(
	})
	if err != nil {
		return nil, fmt.Errorf("error listing instances of %q: %w", crd.Name, err)
	}

	var pending []v1alpha1.ResourceInstance
	for _, cr := range crlist.Items {
		if crd.Spec.Scope != apiextensionsv1.ClusterScoped && !namespaces.Contains(cr.GetNamespace()) {
			continue
		}
		err := crc.Namespace(cr.GetNamespace()).Delete(context.TODO(), cr.GetName(), metav1.DeleteOptions{})
		if errors.IsNotFound(err) {
			continue
		}
		pending = append(pending, v1alpha1.ResourceInstance{
			Name:      cr.GetName(),
			Namespace: cr.GetNamespace(),
		})
		if err != nil {
			return pending, fmt.Errorf("error deleting %q (%s) in namespace %q: %w", cr.GetName(), crd.Name, cr.GetNamespace(), err)
		}
	}
	return pending, nil
}

type customResourceDefinitionGetter interface {
	Get(name string) (*apiextensionsv1.CustomResourceDefinition, error)
}

type deletableResourceDefinitionGetter interface {
	GetDeletableResourceDefinitions(csv *v1alpha1.ClusterServiceVersion) ([]*apiextensionsv1.CustomResourceDefinition, error)
}

type deletableResourceDefinitionGetterImpl struct {
	crdGetter customResourceDefinitionGetter
}

func (g *deletableResourceDefinitionGetterImpl) GetDeletableResourceDefinitions(csv *v1alpha1.ClusterServiceVersion) ([]*apiextensionsv1.CustomResourceDefinition, error) {
	ownedVersionsByName := make(map[string][]string)
	for _, desc := range csv.Spec.CustomResourceDefinitions.Owned {
		ownedVersionsByName[desc.Name] = append(ownedVersionsByName[desc.Name], desc.Version)
	}
	var toDelete []*apiextensionsv1.CustomResourceDefinition
	for name, versions := range ownedVersionsByName {
		crd, err := g.crdGetter.Get(name)
		if err != nil {
			return nil, fmt.Errorf("error fetching crd: %w", err)
		}
		found := true
		for _, existing := range crd.Spec.Versions {
			found = found && func() bool {
				for _, owned := range versions {
					if owned == existing.Name {
						return true
					}
				}
				return false
			}()
		}
		if found {
			toDelete = append(toDelete, crd)
		}
	}
	return toDelete, nil
}

type finalizerRemover interface {
	RemoveFinalizer(csv *v1alpha1.ClusterServiceVersion) error
}

type finalizerRemoverImpl struct {
	client operatorsv1alpha1client.ClusterServiceVersionsGetter
}

func (fr *finalizerRemoverImpl) RemoveFinalizer(csv *v1alpha1.ClusterServiceVersion) error {
	type operation struct {
		Op    string `json:"op"`
		Path  string `json:"path"`
		Value string `json:"value,omitempty"`
	}

	var indices []int
	for i, each := range csv.GetFinalizers() {
		if each == FinalizerDeleteCustomResources {
			indices = append(indices, i)
		}
	}
	if len(indices) == 0 {
		return nil
	}

	var ops []operation
	for _, index := range indices {
		path := fmt.Sprintf("/metadata/finalizers/%d", index)
		ops = append(ops,
			operation{
				Op:    "test",
				Path:  path,
				Value: FinalizerDeleteCustomResources,
			},
			operation{
				Op:   "remove",
				Path: path,
			},
		)
	}
	patch, err := json.Marshal(ops)
	if err != nil {
		return fmt.Errorf("failed to marshal removal patch for finalizer %q: %w", FinalizerDeleteCustomResources, err)
	}

	if _, err := fr.client.ClusterServiceVersions(csv.GetNamespace()).Patch(context.TODO(), csv.GetName(), types.JSONPatchType, patch, metav1.PatchOptions{}); err != nil {
		return fmt.Errorf("failed to remove finalizer %q from csv by patch: %w", FinalizerDeleteCustomResources, err)
	}
	return nil
}
