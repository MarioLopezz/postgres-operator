package resources

import (
	"context"
	"fmt"
	"reflect"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	runtimeClient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	bigdatav1alpha1 "github.com/kubernetesbigdataeg/postgresql-operator/api/v1alpha1"
	"github.com/kubernetesbigdataeg/postgresql-operator/pkg/util"
)

func NewConfigMap(cr *bigdatav1alpha1.Postgresql, scheme *runtime.Scheme) *corev1.ConfigMap {

	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "postgresql-secret",
			Namespace: cr.Namespace,
			Labels:    util.LabelsForPostgresql(cr),
		},
		Data: map[string]string{
			"POSTGRES_DB":       "metastore",
			"POSTGRES_USER":     "postgres",
			"POSTGRES_PASSWORD": "postgres",
		},
	}

	controllerutil.SetControllerReference(cr, configMap, scheme)

	return configMap
}

func ReconcileConfigMap(ctx context.Context, client runtimeClient.Client, desired *corev1.ConfigMap) error {

	log := log.FromContext(ctx)
	log.Info("Reconciling ConfigMap")
	// Get the current ConfigMap
	current := &corev1.ConfigMap{}
	key := runtimeClient.ObjectKeyFromObject(desired)
	err := client.Get(ctx, key, current)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// The ConfigMap does not exist yet, so we'll create it
			log.Info("Creating ConfigMap")
			err = client.Create(ctx, desired)
			if err != nil {
				log.Error(err, "unable to create ConfigMap")
				return fmt.Errorf("unable to create ConfigMap: %v", err)
			} else {
				return nil
			}
		}

		log.Error(err, "error getting ConfigMap")
		return fmt.Errorf("error getting ConfigMap: %v", err)
	}

	// Check if the current ConfigMap matches the desired one
	if !reflect.DeepEqual(current.Data, desired.Data) || !reflect.DeepEqual(current.BinaryData, desired.BinaryData) || !reflect.DeepEqual(current.ObjectMeta.Labels, desired.ObjectMeta.Labels) {
		// If it doesn't match, we'll update the current ConfigMap to match the desired one
		current.Data = desired.Data
		current.BinaryData = desired.BinaryData
		current.ObjectMeta.Labels = desired.ObjectMeta.Labels
		log.Info("Updating ConfigMap")
		err = client.Update(ctx, current)
		if err != nil {
			log.Error(err, "unable to update ConfigMap")
			return fmt.Errorf("unable to update ConfigMap: %v", err)
		}
	}

	// If we reach here, it means the ConfigMap is in the desired state
	return nil
}
