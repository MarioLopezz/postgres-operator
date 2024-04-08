package resources

import (
	"context"
	"fmt"
	"reflect"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	runtimeClient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	bigdatav1alpha1 "github.com/kubernetesbigdataeg/postgresql-operator/api/v1alpha1"
	"github.com/kubernetesbigdataeg/postgresql-operator/pkg/util"
)

func NewStatefulSet(cr *bigdatav1alpha1.Postgresql, scheme *runtime.Scheme) *appsv1.StatefulSet {

	labels := util.LabelsForPostgresql(cr)

	replicas := util.GetSize(cr)

	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cr.Name + "-master",
			Namespace: cr.Namespace,
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: cr.Name + "-master-svc",
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
					Containers: getContainers(cr),
					Volumes:    getVolumes(),
				},
			},
			VolumeClaimTemplates: getVolumeClaimTemplates(cr),
		},
	}

	controllerutil.SetControllerReference(cr, statefulSet, scheme)

	return statefulSet
}

func getContainers(cr *bigdatav1alpha1.Postgresql) []corev1.Container {

	containers := []corev1.Container{
		{
			Image:           util.GetImage(cr),
			Name:            cr.Name,
			ImagePullPolicy: corev1.PullAlways,
			RestartPolicy:   corev1.Always,
			Resources:       getResources(cr),
			Ports: []corev1.ContainerPort{
				{
					Name:          "pgport",
					ContainerPort: 5432,
				},
			},
			Env:          getEnvVars(cr),
			EnvFrom:      getEnvFromSource(cr),
			VolumeMounts: getVolumeMounts(),
		},
	}

	return containers
}

func getEnvVars(cr *bigdatav1alpha1.Postgresql) []corev1.EnvVar {
	return []corev1.EnvVar{
		{
			Name:  "PGDATA",
			Value: "/var/lib/postgresql/data/",
		},
	}
}

func getEnvFromSource(cr *bigdatav1alpha1.Postgresql) []corev1.EnvFromSource {

	return []corev1.EnvFromSource{
		{
			LocalObjectReference: corev1.LocalObjectReference{
				Name: "postgresql-secret",
			},
		},
	}

}

func getVolumes() []corev1.Volume {

	return []corev1.Volume{
		{
			Name: "postgresql-config-volume",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: "postgresql-config",
					},
				},
			},
		},
		{
			Name: "postgresql-logs",
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		},
	}

}

func getVolumeMounts() []corev1.VolumeMount {

	volumeMount := []corev1.VolumeMount{
		{
			Name:      "postgresql-config-volume",
			MountPath: "/etc/environments",
		},
	}

	return volumeMount
}

func getResources(cr *bigdatav1alpha1.Postgresql) corev1.ResourceRequirements {

	if len(cr.Spec.StatefulSet.Resources.Requests) > 0 || len(cr.Spec.StatefulSet.Resources.Limits) > 0 {
		return cr.Spec.StatefulSet.Resources
	} else {
		return corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse(util.CpuRequest),
				corev1.ResourceMemory: resource.MustParse(util.MemoryRequest),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse(util.CpuLimit),
				corev1.ResourceMemory: resource.MustParse(util.MemoryLimit),
			},
		}
	}

}

func getVolumeClaimTemplates(cr *bigdatav1alpha1.Postgresql) []corev1.PersistentVolumeClaim {

	volumeClaimTemplates := []corev1.PersistentVolumeClaim{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "postgresdata",
				Labels: util.labelsForPostgresql(cr),
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("500Mi"),
					},
				},
				StorageClassName: cr.Spec.StatefulSet.StorageClassName,
			},
		},
	}

	return volumeClaimTemplates
}

func ReconcileStatefulSet(ctx context.Context, client runtimeClient.Client, desired *appsv1.StatefulSet) error {

	log := log.FromContext(ctx)
	log.Info("Reconciling StatefulSet")

	// Get the current StatefulSet
	current := &appsv1.StatefulSet{}
	key := runtimeClient.ObjectKeyFromObject(desired)
	err := client.Get(ctx, key, current)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// The StatefulSet does not exist yet, so we'll create it
			log.Info("Creating StatefulSet " + desired.ObjectMeta.Name)
			err = client.Create(ctx, desired)
			if err != nil {
				log.Error(err, "unable to create StatefulSet "+desired.ObjectMeta.Name)
				return fmt.Errorf("unable to create StatefulSet "+desired.ObjectMeta.Name+": %v", err)
			} else {
				return nil
			}
		} else {
			return fmt.Errorf("error getting StatefulSet + desired.ObjectMeta.Name: %v", err)
		}
	}

	// TODO: Check if it has changed in more specific fields.
	// Check if the current StatefulSet matches the desired one
	if !reflect.DeepEqual(current.Spec, desired.Spec) {
		// If it doesn't match, we'll update the current StatefulSet to match the desired one
		current.Spec.Replicas = desired.Spec.Replicas
		current.Spec.Template = desired.Spec.Template
		current.Spec.VolumeClaimTemplates = desired.Spec.VolumeClaimTemplates
		current.Spec.ServiceName = desired.Spec.ServiceName
		log.Info("Updating StatefulSet " + desired.ObjectMeta.Name)
		err = client.Update(ctx, current)
		if err != nil {
			log.Error(err, "unable to update StatefulSet "+desired.ObjectMeta.Name)
			return fmt.Errorf("unable to update StatefulSet "+desired.ObjectMeta.Name+": %v", err)
		}
	}

	// If we reach here, it means the StatefulSet is in the desired state
	return nil
}
