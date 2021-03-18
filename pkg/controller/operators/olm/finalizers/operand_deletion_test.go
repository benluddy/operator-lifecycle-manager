package finalizers

//go:generate go run github.com/golang/mock/mockgen -source operand_deletion.go -package finalizers -destination mock_operand_deletion_test.go -mock_names finalizerRemover=MockFinalizerRemover,customResourceDefinitionGetter=MockCustomResourceDefinitionGetter,deletableResourceDefinitionGetter=MockDeletableResourceDefinitionGetter,resourceDeleter=MockResourceDeleter . finalizerRemover,customResourceDefinitionGetter,deletableResourceDefinitionGetter,resourceDeleter
//go:generate go run github.com/golang/mock/mockgen -package finalizers -destination mock_operatorsv1alpha1_test.go github.com/operator-framework/operator-lifecycle-manager/pkg/api/client/clientset/versioned/typed/operators/v1alpha1 ClusterServiceVersionsGetter,ClusterServiceVersionInterface
//go:generate go run github.com/golang/mock/mockgen -package finalizers -destination mock_dynamic_test.go -mock_names Interface=MockDynamicClient,NamespaceableResourceInterface=MockResourceClient k8s.io/client-go/dynamic Interface,NamespaceableResourceInterface

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	"github.com/operator-framework/api/pkg/operators/v1alpha1"
	resolver "github.com/operator-framework/operator-lifecycle-manager/pkg/controller/registry/resolver"
)

func TestNeedsRequeueDelay(t *testing.T) {
	for _, delay := range []time.Duration{
		33 * time.Minute,
		42 * time.Microsecond,
	} {
		t.Run(delay.String(), func(t *testing.T) {
			assert.Equal(t, delay, NeedsRequeue{delay: delay}.Delay())
		})
	}
}

func TestNeedsRequeueError(t *testing.T) {
	for _, tc := range []struct {
		msg string
		rq  NeedsRequeue
	}{
		{
			msg: "needs requeue after 7s: foobar",
			rq: NeedsRequeue{
				delay:   7 * time.Second,
				wrapped: errors.New("foobar"),
			},
		},
		{
			msg: "needs requeue after 5s",
			rq: NeedsRequeue{
				delay: 5 * time.Second,
			},
		},
	} {
		t.Run(tc.msg, func(t *testing.T) {
			assert.Equal(t, tc.msg, tc.rq.Error())
		})
	}
}

func TestNeedsRequeueUnwrap(t *testing.T) {
	e := errors.New("a")
	assert.Same(t, e, NeedsRequeue{wrapped: e}.Unwrap())
}

func TestDeleteOperands(t *testing.T) {
	now := time.Date(2006, time.March, 21, 22, 50, 0, 0, time.FixedZone("PDT", -7))

	for _, tc := range []struct {
		Name                                   string
		CSV                                    v1alpha1.ClusterServiceVersion
		SetupFinalizerRemover                  func(*MockFinalizerRemoverMockRecorder, *v1alpha1.ClusterServiceVersion)
		SetupDeletableResourceDefinitionGetter func(*MockDeletableResourceDefinitionGetterMockRecorder, *v1alpha1.ClusterServiceVersion)
		SetupResourceDeleter                   func(*MockResourceDeleterMockRecorder)
		StatusUpdated                          bool
		Error                                  error
	}{
		{
			Name: "not being deleted",
			CSV: v1alpha1.ClusterServiceVersion{
				ObjectMeta: metav1.ObjectMeta{
					DeletionTimestamp: nil,
				},
			},
			StatusUpdated: false,
			Error:         nil,
		},
		{
			Name: "finalizer absent",
			CSV: v1alpha1.ClusterServiceVersion{
				ObjectMeta: metav1.ObjectMeta{
					DeletionTimestamp: &metav1.Time{Time: now},
				},
			},
			StatusUpdated: false,
			Error:         nil,
		},
		{
			Name: "disabled by spec",
			CSV: v1alpha1.ClusterServiceVersion{
				ObjectMeta: metav1.ObjectMeta{
					DeletionTimestamp: &metav1.Time{Time: now},
					Finalizers:        []string{"operatorframework.io/delete-custom-resources"},
				},
				Spec: v1alpha1.ClusterServiceVersionSpec{
					Cleanup: v1alpha1.CleanupSpec{
						Enabled: false,
					},
				},
			},
			SetupFinalizerRemover: func(m *MockFinalizerRemoverMockRecorder, csv *v1alpha1.ClusterServiceVersion) {
				m.RemoveFinalizer(csv)
			},
			StatusUpdated: false,
			Error:         nil,
		},
		{
			Name: "error removing finalizer",
			CSV: v1alpha1.ClusterServiceVersion{
				ObjectMeta: metav1.ObjectMeta{
					DeletionTimestamp: &metav1.Time{Time: now},
					Finalizers:        []string{"operatorframework.io/delete-custom-resources"},
				},
				Spec: v1alpha1.ClusterServiceVersionSpec{
					Cleanup: v1alpha1.CleanupSpec{
						Enabled: false,
					},
				},
			},
			SetupFinalizerRemover: func(m *MockFinalizerRemoverMockRecorder, csv *v1alpha1.ClusterServiceVersion) {
				m.RemoveFinalizer(csv).Return(errors.New("expected"))
			},
			StatusUpdated: false,
			Error:         errors.New("expected"),
		},
		{
			Name: "phase is not succeeded",
			CSV: v1alpha1.ClusterServiceVersion{
				ObjectMeta: metav1.ObjectMeta{
					DeletionTimestamp: &metav1.Time{Time: now},
					Finalizers:        []string{"operatorframework.io/delete-custom-resources"},
				},
				Spec: v1alpha1.ClusterServiceVersionSpec{
					Cleanup: v1alpha1.CleanupSpec{
						Enabled: true,
					},
				},
				Status: v1alpha1.ClusterServiceVersionStatus{
					Phase: v1alpha1.CSVPhasePending,
				},
			},
			SetupFinalizerRemover: func(m *MockFinalizerRemoverMockRecorder, csv *v1alpha1.ClusterServiceVersion) {
				m.RemoveFinalizer(csv)
			},
			StatusUpdated: false,
			Error:         nil,
		},
		{
			Name: "error getting deletable definitions",
			CSV: v1alpha1.ClusterServiceVersion{
				ObjectMeta: metav1.ObjectMeta{
					DeletionTimestamp: &metav1.Time{Time: now},
					Finalizers:        []string{"operatorframework.io/delete-custom-resources"},
				},
				Spec: v1alpha1.ClusterServiceVersionSpec{
					Cleanup: v1alpha1.CleanupSpec{
						Enabled: true,
					},
				},
				Status: v1alpha1.ClusterServiceVersionStatus{
					Phase: v1alpha1.CSVPhaseSucceeded,
				},
			},
			SetupDeletableResourceDefinitionGetter: func(m *MockDeletableResourceDefinitionGetterMockRecorder, csv *v1alpha1.ClusterServiceVersion) {
				m.GetDeletableResourceDefinitions(csv).Return(nil, errors.New("expected"))
			},
			StatusUpdated: false,
			Error:         errors.New("error computing set of deletable custom resource types: expected"),
		},
		{
			Name: "resources pending deletion",
			CSV: v1alpha1.ClusterServiceVersion{
				ObjectMeta: metav1.ObjectMeta{
					DeletionTimestamp: &metav1.Time{Time: now},
					Finalizers:        []string{"operatorframework.io/delete-custom-resources"},
				},
				Spec: v1alpha1.ClusterServiceVersionSpec{
					Cleanup: v1alpha1.CleanupSpec{
						Enabled: true,
					},
				},
				Status: v1alpha1.ClusterServiceVersionStatus{
					Phase: v1alpha1.CSVPhaseSucceeded,
				},
			},
			SetupDeletableResourceDefinitionGetter: func(m *MockDeletableResourceDefinitionGetterMockRecorder, csv *v1alpha1.ClusterServiceVersion) {
				m.GetDeletableResourceDefinitions(csv).Return(
					[]*apiextensionsv1.CustomResourceDefinition{
						{
							Spec: apiextensionsv1.CustomResourceDefinitionSpec{
								Names: apiextensionsv1.CustomResourceDefinitionNames{
									Plural: "plural",
								},
								Group: "group",
								Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
									{Name: "v1"},
								},
							},
						},
					},
					nil,
				)
			},
			SetupResourceDeleter: func(m *MockResourceDeleterMockRecorder) {
				m.DeleteResources(gomock.Any(), nil).Return(
					[]v1alpha1.ResourceInstance{
						{Name: "foo"},
					},
					nil,
				)
			},
			StatusUpdated: true,
			Error:         NeedsRequeue{delay: 15 * time.Second},
		},
		{
			Name: "target namespaces from annotation",
			CSV: v1alpha1.ClusterServiceVersion{
				ObjectMeta: metav1.ObjectMeta{
					DeletionTimestamp: &metav1.Time{Time: now},
					Finalizers:        []string{"operatorframework.io/delete-custom-resources"},
					Annotations:       map[string]string{"olm.targetNamespaces": "a,b,c"},
				},
				Spec: v1alpha1.ClusterServiceVersionSpec{
					Cleanup: v1alpha1.CleanupSpec{
						Enabled: true,
					},
				},
				Status: v1alpha1.ClusterServiceVersionStatus{
					Phase: v1alpha1.CSVPhaseSucceeded,
				},
			},
			SetupDeletableResourceDefinitionGetter: func(m *MockDeletableResourceDefinitionGetterMockRecorder, csv *v1alpha1.ClusterServiceVersion) {
				m.GetDeletableResourceDefinitions(csv).Return(
					[]*apiextensionsv1.CustomResourceDefinition{
						{
							Spec: apiextensionsv1.CustomResourceDefinitionSpec{
								Names: apiextensionsv1.CustomResourceDefinitionNames{
									Plural: "plural",
								},
								Group: "group",
								Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
									{Name: "v1"},
								},
							},
						},
					},
					nil,
				)
			},
			SetupResourceDeleter: func(m *MockResourceDeleterMockRecorder) {
				m.DeleteResources(gomock.Any(), resolver.NamespaceSet{"a": struct{}{}, "b": struct{}{}, "c": struct{}{}}).Return(
					[]v1alpha1.ResourceInstance{
						{Name: "foo"},
					},
					nil,
				)
			},
			StatusUpdated: true,
			Error:         NeedsRequeue{delay: 15 * time.Second},
		},
		{
			Name: "resources deletion returns error",
			CSV: v1alpha1.ClusterServiceVersion{
				ObjectMeta: metav1.ObjectMeta{
					DeletionTimestamp: &metav1.Time{Time: now},
					Finalizers:        []string{"operatorframework.io/delete-custom-resources"},
				},
				Spec: v1alpha1.ClusterServiceVersionSpec{
					Cleanup: v1alpha1.CleanupSpec{
						Enabled: true,
					},
				},
				Status: v1alpha1.ClusterServiceVersionStatus{
					Phase: v1alpha1.CSVPhaseSucceeded,
				},
			},
			SetupDeletableResourceDefinitionGetter: func(m *MockDeletableResourceDefinitionGetterMockRecorder, csv *v1alpha1.ClusterServiceVersion) {
				m.GetDeletableResourceDefinitions(csv).Return(
					[]*apiextensionsv1.CustomResourceDefinition{
						{
							ObjectMeta: metav1.ObjectMeta{
								Name: "plural.group",
							},
							Spec: apiextensionsv1.CustomResourceDefinitionSpec{
								Names: apiextensionsv1.CustomResourceDefinitionNames{
									Plural: "plural",
								},
								Group: "group",
								Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
									{Name: "v1"},
								},
							},
						},
					},
					nil,
				)
			},
			SetupResourceDeleter: func(m *MockResourceDeleterMockRecorder) {
				m.DeleteResources(gomock.Any(), nil).Return(nil, errors.New("expected"))
			},
			StatusUpdated: true,
			Error:         errors.New(`error deleting instances of crd "plural.group": expected`),
		},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			ctrl := gomock.NewController(t)

			deleter := OperandDeleter{}
			deleter.logger, _ = test.NewNullLogger()

			finalizerRemover := NewMockFinalizerRemover(ctrl)
			if tc.SetupFinalizerRemover != nil {
				tc.SetupFinalizerRemover(finalizerRemover.EXPECT(), &tc.CSV)
			}
			deleter.finalizerRemover = finalizerRemover

			deletableResourceDefinitionGetter := NewMockDeletableResourceDefinitionGetter(ctrl)
			if tc.SetupDeletableResourceDefinitionGetter != nil {
				tc.SetupDeletableResourceDefinitionGetter(deletableResourceDefinitionGetter.EXPECT(), &tc.CSV)
			}
			deleter.deletableResourceDefinitionGetter = deletableResourceDefinitionGetter

			resourceDeleter := NewMockResourceDeleter(ctrl)
			if tc.SetupResourceDeleter != nil {
				tc.SetupResourceDeleter(resourceDeleter.EXPECT())
			}
			deleter.resourceDeleter = resourceDeleter

			var status v1alpha1.CleanupStatus
			deleting, err := deleter.DeleteOperands(&tc.CSV, &status)
			assert := assert.New(t)
			assert.Equal(tc.StatusUpdated, deleting)
			if tc.Error == nil {
				assert.NoError(err)
			} else {
				assert.EqualError(err, tc.Error.Error())
			}
		})
	}
}

func TestDeleteResources(t *testing.T) {
	for _, tc := range []struct {
		Name               string
		CRD                apiextensionsv1.CustomResourceDefinition
		Namespaces         resolver.NamespaceSet
		SetupDynamicClient func(*MockDynamicClient, *MockResourceClient)
		Instances          []v1alpha1.ResourceInstance
		Error              error
	}{
		{
			Name: "no versions",
		},
		{
			Name: "no served versions",
			CRD: apiextensionsv1.CustomResourceDefinition{
				Spec: apiextensionsv1.CustomResourceDefinitionSpec{
					Names: apiextensionsv1.CustomResourceDefinitionNames{
						Plural: "plural",
					},
					Group: "group",
					Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
						{Name: "v1", Served: false},
					},
				},
			},
		},
		{
			Name: "no instances",
			CRD: apiextensionsv1.CustomResourceDefinition{
				Spec: apiextensionsv1.CustomResourceDefinitionSpec{
					Names: apiextensionsv1.CustomResourceDefinitionNames{
						Plural: "plural",
					},
					Group: "group",
					Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
						{Name: "v1", Served: true},
					},
				},
			},
			SetupDynamicClient: func(d *MockDynamicClient, r *MockResourceClient) {
				gomock.InOrder(
					d.EXPECT().Resource(schema.GroupVersionResource{
						Resource: "plural",
						Group:    "group",
						Version:  "v1",
					}).Return(r),
					r.EXPECT().List(context.TODO(), metav1.ListOptions{Limit: 200}).Return(
						&unstructured.UnstructuredList{}, nil,
					),
				)
			},
		},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			ctrl := gomock.NewController(t)

			client := NewMockDynamicClient(ctrl)
			if tc.SetupDynamicClient != nil {
				tc.SetupDynamicClient(client, NewMockResourceClient(ctrl))
			}
			rd := resourceDeleterImpl{
				client: client,
			}

			instances, err := rd.DeleteResources(&tc.CRD, tc.Namespaces)

			assert := assert.New(t)
			assert.Equal(tc.Instances, instances)
			if tc.Error == nil {
				assert.NoError(err)
			} else {
				assert.EqualError(err, tc.Error.Error())
			}
		})
	}
}

func EqByteString(want string) gomock.Matcher {
	return gomock.GotFormatterAdapter(
		gomock.GotFormatterFunc(func(got interface{}) string {
			return string(got.([]byte))
		}),
		gomock.Eq([]byte(want)),
	)
}

func TestGetDeletableResourceDefinitions(t *testing.T) {
	for _, tc := range []struct {
		Name                                string
		CSV                                 v1alpha1.ClusterServiceVersion
		SetupCustomResourceDefinitionGetter func(*MockCustomResourceDefinitionGetterMockRecorder)
		CRDs                                []*apiextensionsv1.CustomResourceDefinition
		Error                               error
	}{
		{
			Name: "no owned crds",
		},
		{
			Name: "all versions owned",
			CSV: v1alpha1.ClusterServiceVersion{
				Spec: v1alpha1.ClusterServiceVersionSpec{
					CustomResourceDefinitions: v1alpha1.CustomResourceDefinitions{
						Owned: []v1alpha1.CRDDescription{
							{
								Name:    "foo",
								Version: "v1",
							},
						},
					},
				},
			},
			SetupCustomResourceDefinitionGetter: func(m *MockCustomResourceDefinitionGetterMockRecorder) {
				m.Get("foo").Return(
					&apiextensionsv1.CustomResourceDefinition{
						ObjectMeta: metav1.ObjectMeta{Name: "foo"},
						Spec: apiextensionsv1.CustomResourceDefinitionSpec{
							Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
								{Name: "v1"},
							},
						},
					},
					nil,
				)
			},
			CRDs: []*apiextensionsv1.CustomResourceDefinition{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "foo"},
					Spec: apiextensionsv1.CustomResourceDefinitionSpec{
						Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
							{Name: "v1"},
						},
					},
				},
			},
		},
		{
			Name: "not all versions owned",
			CSV: v1alpha1.ClusterServiceVersion{
				Spec: v1alpha1.ClusterServiceVersionSpec{
					CustomResourceDefinitions: v1alpha1.CustomResourceDefinitions{
						Owned: []v1alpha1.CRDDescription{
							{
								Name:    "foo",
								Version: "v1",
							},
						},
					},
				},
			},
			SetupCustomResourceDefinitionGetter: func(m *MockCustomResourceDefinitionGetterMockRecorder) {
				m.Get("foo").Return(
					&apiextensionsv1.CustomResourceDefinition{
						ObjectMeta: metav1.ObjectMeta{Name: "foo"},
						Spec: apiextensionsv1.CustomResourceDefinitionSpec{
							Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
								{Name: "v1"},
								{Name: "v2"},
							},
						},
					},
					nil,
				)
			},
		},
		{
			Name: "error getting crd",
			CSV: v1alpha1.ClusterServiceVersion{
				Spec: v1alpha1.ClusterServiceVersionSpec{
					CustomResourceDefinitions: v1alpha1.CustomResourceDefinitions{
						Owned: []v1alpha1.CRDDescription{
							{
								Name:    "foo",
								Version: "v1",
							},
						},
					},
				},
			},
			SetupCustomResourceDefinitionGetter: func(m *MockCustomResourceDefinitionGetterMockRecorder) {
				m.Get("foo").Return(nil, errors.New("expected"))
			},
			Error: errors.New("error fetching crd: expected"),
		},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			ctrl := gomock.NewController(t)

			crdGetter := NewMockCustomResourceDefinitionGetter(ctrl)
			if tc.SetupCustomResourceDefinitionGetter != nil {
				tc.SetupCustomResourceDefinitionGetter(crdGetter.EXPECT())
			}
			g := &deletableResourceDefinitionGetterImpl{
				crdGetter: crdGetter,
			}
			crds, err := g.GetDeletableResourceDefinitions(&tc.CSV)

			assert := assert.New(t)
			assert.Equal(tc.CRDs, crds)
			if tc.Error == nil {
				assert.NoError(err)
			} else {
				assert.EqualError(err, tc.Error.Error())
			}
		})
	}
}

func TestRemoveFinalizer(t *testing.T) {
	for _, tc := range []struct {
		Name        string
		CSV         v1alpha1.ClusterServiceVersion
		SetupClient func(*MockClusterServiceVersionsGetterMockRecorder, *MockClusterServiceVersionInterface)
		Error       error
	}{
		{
			Name:  "not found",
			CSV:   v1alpha1.ClusterServiceVersion{},
			Error: nil,
		},
		{
			Name: "found",
			CSV: v1alpha1.ClusterServiceVersion{
				ObjectMeta: metav1.ObjectMeta{
					Namespace:  "foo",
					Name:       "bar",
					Finalizers: []string{"operatorframework.io/delete-custom-resources"},
				},
			},
			SetupClient: func(m *MockClusterServiceVersionsGetterMockRecorder, i *MockClusterServiceVersionInterface) {
				m.ClusterServiceVersions("foo").Return(i)
				i.EXPECT().Patch(context.TODO(), "bar", types.JSONPatchType, EqByteString(`[{"op":"test","path":"/metadata/finalizers/0","value":"operatorframework.io/delete-custom-resources"},{"op":"remove","path":"/metadata/finalizers/0"}]`), metav1.PatchOptions{}).Return(nil, nil)
			},
			Error: nil,
		},
		{
			Name: "multiple found",
			CSV: v1alpha1.ClusterServiceVersion{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
					Finalizers: []string{
						"operatorframework.io/delete-custom-resources",
						"baz",
						"operatorframework.io/delete-custom-resources",
					},
				},
			},
			SetupClient: func(m *MockClusterServiceVersionsGetterMockRecorder, i *MockClusterServiceVersionInterface) {
				m.ClusterServiceVersions("foo").Return(i)
				i.EXPECT().Patch(context.TODO(), "bar", types.JSONPatchType, EqByteString(`[{"op":"test","path":"/metadata/finalizers/0","value":"operatorframework.io/delete-custom-resources"},{"op":"remove","path":"/metadata/finalizers/0"},{"op":"test","path":"/metadata/finalizers/2","value":"operatorframework.io/delete-custom-resources"},{"op":"remove","path":"/metadata/finalizers/2"}]`), metav1.PatchOptions{}).Return(nil, nil)
			},
			Error: nil,
		},
		{
			Name: "patch error",
			CSV: v1alpha1.ClusterServiceVersion{
				ObjectMeta: metav1.ObjectMeta{
					Namespace:  "foo",
					Name:       "bar",
					Finalizers: []string{"operatorframework.io/delete-custom-resources"},
				},
			},
			SetupClient: func(m *MockClusterServiceVersionsGetterMockRecorder, i *MockClusterServiceVersionInterface) {
				m.ClusterServiceVersions("foo").Return(i)
				i.EXPECT().Patch(context.TODO(), "bar", types.JSONPatchType, EqByteString(`[{"op":"test","path":"/metadata/finalizers/0","value":"operatorframework.io/delete-custom-resources"},{"op":"remove","path":"/metadata/finalizers/0"}]`), metav1.PatchOptions{}).Return(nil, errors.New("expected"))
			},
			Error: errors.New(`failed to remove finalizer "operatorframework.io/delete-custom-resources" from csv by patch: expected`),
		},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			ctrl := gomock.NewController(t)

			client := NewMockClusterServiceVersionsGetter(ctrl)
			if tc.SetupClient != nil {
				namespaced := NewMockClusterServiceVersionInterface(ctrl)
				tc.SetupClient(client.EXPECT(), namespaced)
			}
			r := finalizerRemoverImpl{
				client: client,
			}

			if tc.Error == nil {
				assert.NoError(t, r.RemoveFinalizer(&tc.CSV))
			} else {
				assert.EqualError(t, r.RemoveFinalizer(&tc.CSV), tc.Error.Error())
			}
		})
	}
}
