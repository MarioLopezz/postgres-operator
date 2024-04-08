/*
Copyright 2023.

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

package controllers

import (
	"context"
	"fmt"
	"os"

	"strings"

	"k8s.io/apimachinery/pkg/api/resource"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	bigdatav1alpha1 "github.com/kubernetesbigdataeg/postgresql-operator/api/v1alpha1"
)

const postgresFinalizer = "bigdata.kubernetesbigdataeg.org/finalizer"

// Definitions to manage status conditions
const (
	// typeAvailablePostgresql represents the status of the Deployment reconciliation
	typeAvailablePostgresql = "Available"
	// typeDegradedPostgresql represents the status used when the custom resource is deleted and the finalizer operations are must to occur.
	typeDegradedPostgresql = "Degraded"
)

// PostgresqlReconciler reconciles a Postgresql object
type PostgresqlReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

// The following markers are used to generate the rules permissions (RBAC) on config/rbac using controller-gen
// when the command <make manifests> is executed.
// To know more about markers see: https://book.kubebuilder.io/reference/markers.html

//+kubebuilder:rbac:groups=bigdata.kubernetesbigdataeg.org,resources=postgresql,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=bigdata.kubernetesbigdataeg.org,resources=postgresql/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=bigdata.kubernetesbigdataeg.org,resources=postgresql/finalizers,verbs=update
//+kubebuilder:rbac:groups=core,resources=events,verbs=create;patch
//+kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch
//+kubebuilder:rbac:groups=core,resources=configmaps;services,verbs=get;list;create;watch
//+kubebuilder:rbac:groups=apps,resources=deployments;statefulsets;configmaps,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.

// It is essential for the controller's reconciliation loop to be idempotent. By following the Operator
// pattern you will create Controllers which provide a reconcile function
// responsible for synchronizing resources until the desired state is reached on the cluster.
// Breaking this recommendation goes against the design principles of controller-runtime.
// and may lead to unforeseen consequences such as resources becoming stuck and requiring manual intervention.
// For further info:
// - About Operator Pattern: https://kubernetes.io/docs/concepts/extend-kubernetes/operator/
// - About Controllers: https://kubernetes.io/docs/concepts/architecture/controller/
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.13.0/pkg/reconcile
func (r *PostgresqlReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	//
	// 1. Control-loop: checking if Postgresql CR exists
	//
	// Fetch the Postgresql instance
	// The purpose is check if the Custom Resource for the Kind Postgresql
	// is applied on the cluster if not we return nil to stop the reconciliation
	postgresql := &bigdatav1alpha1.Postgresql{}
	err := r.Get(ctx, req.NamespacedName, postgresql)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// If the custom resource is not found then, it usually means that it was deleted or not created
			// In this way, we will stop the reconciliation
			log.Info("postgresql resource not found. Ignoring since object must be deleted")
			return ctrl.Result{}, nil
		}
		// Error reading the object - requeue the request.
		log.Error(err, "Failed to get postgresql")
		return ctrl.Result{}, err
	}

	//
	// 2. Control-loop: Status to Unknown
	//
	// Let's just set the status as Unknown when no status are available
	if postgresql.Status.Conditions == nil || len(postgresql.Status.Conditions) == 0 {
		meta.SetStatusCondition(&postgresql.Status.Conditions, metav1.Condition{Type: typeAvailablePostgresql, Status: metav1.ConditionUnknown, Reason: "Reconciling", Message: "Starting reconciliation"})
		if err = r.Status().Update(ctx, postgresql); err != nil {
			log.Error(err, "Failed to update Postgresql status")
			return ctrl.Result{}, err
		}

		// Let's re-fetch the postgresql Custom Resource after update the status
		// so that we have the latest state of the resource on the cluster and we will avoid
		// raise the issue "the object has been modified, please apply
		// your changes to the latest version and try again" which would re-trigger the reconciliation
		// if we try to update it again in the following operations
		if err := r.Get(ctx, req.NamespacedName, postgresql); err != nil {
			log.Error(err, "Failed to re-fetch postgresql")
			return ctrl.Result{}, err
		}
	}

	//
	// 3. Control-loop: Let's add a finalizer
	//
	// Let's add a finalizer. Then, we can define some operations which should
	// occurs before the custom resource to be deleted.
	// More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/finalizers
	if !controllerutil.ContainsFinalizer(postgresql, postgresFinalizer) {
		log.Info("Adding Finalizer for Postgresql")
		if ok := controllerutil.AddFinalizer(postgresql, postgresFinalizer); !ok {
			log.Error(err, "Failed to add finalizer into the custom resource")
			return ctrl.Result{Requeue: true}, nil
		}

		if err = r.Update(ctx, postgresql); err != nil {
			log.Error(err, "Failed to update custom resource to add finalizer")
			return ctrl.Result{}, err
		}
	}

	//
	// 4. Control-loop: Instance marked for deletion
	//
	// Check if the Postgresql instance is marked to be deleted, which is
	// indicated by the deletion timestamp being set.
	isPostgresqlMarkedToBeDeleted := postgresql.GetDeletionTimestamp() != nil
	if isPostgresqlMarkedToBeDeleted {
		if controllerutil.ContainsFinalizer(postgresql, postgresFinalizer) {
			log.Info("Performing Finalizer Operations for Postgresql before delete CR")

			// Let's add here an status "Downgrade" to define that this resource begin its process to be terminated.
			meta.SetStatusCondition(&postgresql.Status.Conditions, metav1.Condition{Type: typeDegradedPostgresql,
				Status: metav1.ConditionUnknown, Reason: "Finalizing",
				Message: fmt.Sprintf("Performing finalizer operations for the custom resource: %s ", postgresql.Name)})

			if err := r.Status().Update(ctx, postgresql); err != nil {
				log.Error(err, "Failed to update Postgresql status")
				return ctrl.Result{}, err
			}

			// Perform all operations required before remove the finalizer and allow
			// the Kubernetes API to remove the custom resource.
			r.doFinalizerOperationsForPostgresql(postgresql)

			// TODO(user): If you add operations to the doFinalizerOperationsForPostgresql method
			// then you need to ensure that all worked fine before deleting and updating the Downgrade status
			// otherwise, you should requeue here.

			// Re-fetch the postgresql Custom Resource before update the status
			// so that we have the latest state of the resource on the cluster and we will avoid
			// raise the issue "the object has been modified, please apply
			// your changes to the latest version and try again" which would re-trigger the reconciliation
			if err := r.Get(ctx, req.NamespacedName, postgresql); err != nil {
				log.Error(err, "Failed to re-fetch postgresql")
				return ctrl.Result{}, err
			}

			meta.SetStatusCondition(&postgresql.Status.Conditions, metav1.Condition{Type: typeDegradedPostgresql,
				Status: metav1.ConditionTrue, Reason: "Finalizing",
				Message: fmt.Sprintf("Finalizer operations for custom resource %s name were successfully accomplished", postgresql.Name)})

			if err := r.Status().Update(ctx, postgresql); err != nil {
				log.Error(err, "Failed to update Postgresql status")
				return ctrl.Result{}, err
			}

			log.Info("Removing Finalizer for Postgresql after successfully perform the operations")
			if ok := controllerutil.RemoveFinalizer(postgresql, postgresFinalizer); !ok {
				log.Error(err, "Failed to remove finalizer for Postgresql")
				return ctrl.Result{Requeue: true}, nil
			}

			if err := r.Update(ctx, postgresql); err != nil {
				log.Error(err, "Failed to remove finalizer for Postgresql")
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	//
	// 5. Control-loop: Let's deploy/ensure our managed resources for Postgresql
	// - ConfigMap,
	// - Service ClusterIP,
	// - StateFulSet,
	//

	// ConfigMap
	configMapFound := &corev1.ConfigMap{}
	if err := r.ensureResource(ctx, postgresql, r.defaultConfigMapForPostgresql, configMapFound, "postgresql-secret", "ConfigMap"); err != nil {
		return ctrl.Result{}, err
	}

	// Service
	serviceFound := &corev1.Service{}
	if err := r.ensureResource(ctx, postgresql, r.serviceForPostgresql, serviceFound, "postgresql-svc", "Service"); err != nil {
		return ctrl.Result{}, err
	}

	// Deployment
	stateFulSetFound := &appsv1.StatefulSet{}
	if err := r.ensureResource(ctx, postgresql, r.stateFulSetForPostgresql, stateFulSetFound, postgresql.Name, "StateFulSet"); err != nil {
		return ctrl.Result{}, err
	}

	//
	// 6. Control-loop: Check the number of replicas
	//
	// The CRD API is defining that the Postgresql type, have a PostgresqlSpec.Size field
	// to set the quantity of StateFulSet instances is the desired state on the cluster.
	// Therefore, the following code will ensure the StateFulSet size is the same as defined
	// via the Size spec of the Custom Resource which we are reconciling.
	size := postgresql.Spec.Size
	if stateFulSetFound.Spec.Replicas == nil {
		log.Error(nil, "Spec is not initialized for StateFulSet", "StateFulSet.Namespace", stateFulSetFound.Namespace, "StateFulSet.Name", stateFulSetFound.Name)
		return ctrl.Result{}, fmt.Errorf("spec is not initialized for StateFulSet %s/%s", stateFulSetFound.Namespace, stateFulSetFound.Name)
	}
	if *stateFulSetFound.Spec.Replicas != size {
		stateFulSetFound.Spec.Replicas = &size
		if err = r.Update(ctx, stateFulSetFound); err != nil {
			log.Error(err, "Failed to update StateFulSet",
				"StateFulSet.Namespace", stateFulSetFound.Namespace, "StateFulSet.Name", stateFulSetFound.Name)

			// Re-fetch the postgresql Custom Resource before update the status
			// so that we have the latest state of the resource on the cluster and we will avoid
			// raise the issue "the object has been modified, please apply
			// your changes to the latest version and try again" which would re-trigger the reconciliation
			if err := r.Get(ctx, req.NamespacedName, postgresql); err != nil {
				log.Error(err, "Failed to re-fetch postgresql")
				return ctrl.Result{}, err
			}

			// The following implementation will update the status
			meta.SetStatusCondition(&postgresql.Status.Conditions, metav1.Condition{Type: typeAvailablePostgresql,
				Status: metav1.ConditionFalse, Reason: "Resizing",
				Message: fmt.Sprintf("Failed to update the size for the custom resource (%s): (%s)", postgresql.Name, err)})

			if err := r.Status().Update(ctx, postgresql); err != nil {
				log.Error(err, "Failed to update Postgresql status")
				return ctrl.Result{}, err
			}

			return ctrl.Result{}, err
		}

		// Now, that we update the size we want to requeue the reconciliation
		// so that we can ensure that we have the latest state of the resource before
		// update. Also, it will help ensure the desired state on the cluster
		return ctrl.Result{Requeue: true}, nil
	}

	//
	// 7. Control-loop: Let's update the status
	//
	// The following implementation will update the status
	meta.SetStatusCondition(&postgresql.Status.Conditions, metav1.Condition{Type: typeAvailablePostgresql,
		Status: metav1.ConditionTrue, Reason: "Reconciling",
		Message: fmt.Sprintf("Deployment for custom resource (%s) with %d replicas created successfully", postgresql.Name, size)})

	if err := r.Status().Update(ctx, postgresql); err != nil {
		log.Error(err, "Failed to update Postgresql status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// finalizePostgresql will perform the required operations before delete the CR.
func (r *PostgresqlReconciler) doFinalizerOperationsForPostgresql(cr *bigdatav1alpha1.Postgresql) {
	// TODO(user): Add the cleanup steps that the operator
	// needs to do before the CR can be deleted. Examples
	// of finalizers include performing backups and deleting
	// resources that are not owned by this CR, like a PVC.

	// Note: It is not recommended to use finalizers with the purpose of delete resources which are
	// created and managed in the reconciliation. These ones, such as the Deployment created on this reconcile,
	// are defined as depended of the custom resource. See that we use the method ctrl.SetControllerReference.
	// to set the ownerRef which means that the Deployment will be deleted by the Kubernetes API.
	// More info: https://kubernetes.io/docs/tasks/administer-cluster/use-cascading-deletion/

	// The following implementation will raise an event
	r.Recorder.Event(cr, "Warning", "Deleting",
		fmt.Sprintf("Custom Resource %s is being deleted from the namespace %s",
			cr.Name,
			cr.Namespace))
}

func (r *PostgresqlReconciler) defaultConfigMapForPostgresql(
	v *bigdatav1alpha1.Postgresql, resourceName string) (client.Object, error) {

	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      resourceName,
			Namespace: v.Namespace,
			Labels: map[string]string{
				"app": "postgresql",
			},
		},
		Data: map[string]string{
			"POSTGRES_DB":       "metastore",
			"POSTGRES_USER":     "postgres",
			"POSTGRES_PASSWORD": "postgres",
		},
	}

	if err := ctrl.SetControllerReference(v, configMap, r.Scheme); err != nil {
		return nil, err
	}

	return configMap, nil
}

func (r *PostgresqlReconciler) serviceForPostgresql(
	postgresql *bigdatav1alpha1.Postgresql, resourceName string) (client.Object, error) {

	labels := labelsForPostgresql(postgresql.Name)
	s := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      resourceName,
			Namespace: postgresql.Namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			Selector: labels,
			Ports: []corev1.ServicePort{{
				Name: "postgresql-port",
				Port: 5432,
			}},
			Type: corev1.ServiceTypeClusterIP,
		},
	}

	if err := ctrl.SetControllerReference(postgresql, s, r.Scheme); err != nil {
		return nil, err
	}

	return s, nil
}

// stateFulSetForPostgresql returns a Postgresql StateFulSet object
func (r *PostgresqlReconciler) stateFulSetForPostgresql(
	postgresql *bigdatav1alpha1.Postgresql, resourceName string) (client.Object, error) {

	labels := labelsForPostgresql(postgresql.Name)

	replicas := postgresql.Spec.Size

	// Get the Operand image
	image, err := imageForPostgresql()
	if err != nil {
		return nil, err
	}

	fastdisks := "fast-disks"

	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      resourceName,
			Namespace: postgresql.Namespace,
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: "postgresql-svc",
			Replicas:    &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
				Type:          "RollingUpdate",
				RollingUpdate: nil,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Image:           image,
						Name:            "postgresql",
						ImagePullPolicy: corev1.PullIfNotPresent,
						Ports: []corev1.ContainerPort{
							{
								ContainerPort: 5432,
								Name:          "postgresql-port",
							}},
						VolumeMounts: []corev1.VolumeMount{
							{
								Name:      "postgresdata",
								MountPath: "/var/lib/postgresql/data/",
							},
						},
						Env: []corev1.EnvVar{
							{
								Name:  "PGDATA",
								Value: "/var/lib/postgresql/data/",
							},
						},
						EnvFrom: []corev1.EnvFromSource{
							{
								ConfigMapRef: &corev1.ConfigMapEnvSource{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: "postgresql-secret",
									},
								},
							},
						},
					}},
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "postgresdata",
					Labels: labels,
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("500Mi"),
						},
					},
					StorageClassName: &fastdisks,
				},
			}},
		},
	}

	// Set the ownerRef for the Deployment
	// More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/owners-dependents/
	if err := ctrl.SetControllerReference(postgresql, sts, r.Scheme); err != nil {
		return nil, err
	}
	return sts, nil
}

// labelsForPostgresql returns the labels for selecting the resources
// More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/common-labels/
func labelsForPostgresql(name string) map[string]string {
	var imageTag string
	image, err := imageForPostgresql()
	if err == nil {
		imageTag = strings.Split(image, ":")[1]
	}
	return map[string]string{
		"app.kubernetes.io/name":       "Postgresql",
		"app.kubernetes.io/instance":   name,
		"app.kubernetes.io/version":    imageTag,
		"app.kubernetes.io/part-of":    "postgresql-operator",
		"app.kubernetes.io/created-by": "controller-manager",
		"app":                          "postgresql",
	}
}

// imageForPostgresql gets the Operand image which is managed by this controller
// from the POSTGRES_IMAGE environment variable defined in the config/manager/manager.yaml
func imageForPostgresql() (string, error) {
	var imageEnvVar = "POSTGRES_IMAGE"
	image, found := os.LookupEnv(imageEnvVar)
	if !found {
		return "", fmt.Errorf("unable to find %s environment variable with the image", imageEnvVar)
	}
	return image, nil
}

// SetupWithManager sets up the controller with the Manager.
// Note that the Deployment will be also watched in order to ensure its
// desirable state on the cluster
func (r *PostgresqlReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&bigdatav1alpha1.Postgresql{}).
		Owns(&appsv1.Deployment{}).
		Complete(r)
}

func (r *PostgresqlReconciler) ensureResource(ctx context.Context, postgresql *bigdatav1alpha1.Postgresql, createResourceFunc func(*bigdatav1alpha1.Postgresql, string) (client.Object, error), foundResource client.Object, resourceName string, resourceType string) error {
	log := log.FromContext(ctx)
	err := r.Get(ctx, types.NamespacedName{Name: resourceName, Namespace: postgresql.Namespace}, foundResource)
	if err != nil && apierrors.IsNotFound(err) {
		resource, err := createResourceFunc(postgresql, resourceName)
		if err != nil {
			log.Error(err, fmt.Sprintf("Failed to define new %s resource for Postgresql", resourceType))

			// The following implementation will update the status
			meta.SetStatusCondition(&postgresql.Status.Conditions, metav1.Condition{Type: typeAvailablePostgresql,
				Status: metav1.ConditionFalse, Reason: "Reconciling",
				Message: fmt.Sprintf("Failed to create %s for the custom resource (%s): (%s)", resourceType, postgresql.Name, err)})

			if err := r.Status().Update(ctx, postgresql); err != nil {
				log.Error(err, "Failed to update Postgresql status")
				return err
			}

			return err
		}

		log.Info(fmt.Sprintf("Creating a new %s", resourceType),
			fmt.Sprintf("%s.Namespace", resourceType), resource.GetNamespace(), fmt.Sprintf("%s.Name", resourceType), resource.GetName())

		if err = r.Create(ctx, resource); err != nil {
			log.Error(err, fmt.Sprintf("Failed to create new %s", resourceType),
				fmt.Sprintf("%s.Namespace", resourceType), resource.GetNamespace(), fmt.Sprintf("%s.Name", resourceType), resource.GetName())
			return err
		}

		if err := r.Get(ctx, types.NamespacedName{Name: resourceName, Namespace: postgresql.Namespace}, foundResource); err != nil {
			log.Error(err, fmt.Sprintf("Failed to get newly created %s", resourceType))
			return err
		}

	} else if err != nil {
		log.Error(err, fmt.Sprintf("Failed to get %s", resourceType))
		return err
	}

	return nil
}
