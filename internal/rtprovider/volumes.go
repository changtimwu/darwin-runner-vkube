package rtprovider

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"

	runtimev1 "k3sm.io/apis/runtime/v1"
)

// toVolumes translates the pod-ephemeral volume kinds runtimed renders into
// the pod's data volume. Anything else is refused up front: a pod that
// silently ran without its volume would be worse than one that never starts.
func toVolumes(vs []corev1.Volume) ([]*runtimev1.Volume, error) {
	out := make([]*runtimev1.Volume, 0, len(vs))
	for _, v := range vs {
		rv := &runtimev1.Volume{Name: v.Name}
		switch s := v.VolumeSource; {
		case s.ConfigMap != nil:
			rv.ConfigMap = &runtimev1.ConfigMapVolumeSource{
				Name: s.ConfigMap.Name, Items: toKeyToPaths(s.ConfigMap.Items),
				DefaultMode: deref32(s.ConfigMap.DefaultMode), Optional: derefBool(s.ConfigMap.Optional),
			}
		case s.Secret != nil:
			rv.Secret = &runtimev1.SecretVolumeSource{
				SecretName: s.Secret.SecretName, Items: toKeyToPaths(s.Secret.Items),
				DefaultMode: deref32(s.Secret.DefaultMode), Optional: derefBool(s.Secret.Optional),
			}
		case s.EmptyDir != nil:
			rv.EmptyDir = &runtimev1.EmptyDirVolumeSource{Medium: string(s.EmptyDir.Medium)}
			if s.EmptyDir.SizeLimit != nil {
				rv.EmptyDir.SizeLimit = s.EmptyDir.SizeLimit.String()
			}
		case s.DownwardAPI != nil:
			files, err := toDownwardFiles(v.Name, s.DownwardAPI.Items)
			if err != nil {
				return nil, err
			}
			rv.DownwardApi = &runtimev1.DownwardAPIVolumeSource{Items: files, DefaultMode: deref32(s.DownwardAPI.DefaultMode)}
		case s.Projected != nil:
			p := &runtimev1.ProjectedVolumeSource{DefaultMode: deref32(s.Projected.DefaultMode)}
			for _, src := range s.Projected.Sources {
				vp := &runtimev1.VolumeProjection{}
				switch {
				case src.ConfigMap != nil:
					vp.ConfigMap = &runtimev1.ConfigMapProjection{Name: src.ConfigMap.Name, Items: toKeyToPaths(src.ConfigMap.Items), Optional: derefBool(src.ConfigMap.Optional)}
				case src.Secret != nil:
					vp.Secret = &runtimev1.SecretProjection{Name: src.Secret.Name, Items: toKeyToPaths(src.Secret.Items), Optional: derefBool(src.Secret.Optional)}
				case src.DownwardAPI != nil:
					files, err := toDownwardFiles(v.Name, src.DownwardAPI.Items)
					if err != nil {
						return nil, err
					}
					vp.DownwardApi = &runtimev1.DownwardAPIProjection{Items: files}
				case src.ServiceAccountToken != nil:
					t := src.ServiceAccountToken
					vp.ServiceAccountToken = &runtimev1.ServiceAccountTokenProjection{Audience: t.Audience, ExpirationSeconds: deref(t.ExpirationSeconds), Path: t.Path}
				default:
					return nil, fmt.Errorf("volume %s: unsupported projected source (clusterTrustBundle and podCertificate are not supported)", v.Name)
				}
				p.Sources = append(p.Sources, vp)
			}
			rv.Projected = p
		case s.HostPath != nil:
			return nil, fmt.Errorf("volume %s: hostPath is not supported on darwin nodes", v.Name)
		case s.PersistentVolumeClaim != nil:
			return nil, fmt.Errorf("volume %s: persistentVolumeClaim is not supported on darwin nodes yet", v.Name)
		default:
			return nil, fmt.Errorf("volume %s: unsupported volume type (darwin nodes support configMap, secret, emptyDir, downwardAPI and projected)", v.Name)
		}
		out = append(out, rv)
	}
	return out, nil
}

func toKeyToPaths(items []corev1.KeyToPath) []*runtimev1.KeyToPath {
	out := make([]*runtimev1.KeyToPath, 0, len(items))
	for _, it := range items {
		out = append(out, &runtimev1.KeyToPath{Key: it.Key, Path: it.Path, Mode: deref32(it.Mode)})
	}
	return out
}

// toDownwardFiles keeps fieldRef items. resourceFieldRef needs values only
// the node computes, so it is refused rather than written empty.
func toDownwardFiles(volume string, items []corev1.DownwardAPIVolumeFile) ([]*runtimev1.DownwardAPIVolumeFile, error) {
	out := make([]*runtimev1.DownwardAPIVolumeFile, 0, len(items))
	for _, it := range items {
		if it.FieldRef == nil {
			return nil, fmt.Errorf("volume %s: downwardAPI item %s: only fieldRef is supported", volume, it.Path)
		}
		if f := it.FieldRef.FieldPath; f == "metadata.labels" || f == "metadata.annotations" {
			return nil, fmt.Errorf("volume %s: downwardAPI item %s: %s as a whole is not supported; use %s['key']", volume, it.Path, f, f)
		}
		out = append(out, &runtimev1.DownwardAPIVolumeFile{
			Path:     it.Path,
			FieldRef: &runtimev1.ObjectFieldSelector{ApiVersion: it.FieldRef.APIVersion, FieldPath: it.FieldRef.FieldPath},
			Mode:     deref32(it.Mode),
		})
	}
	return out, nil
}

func toVolumeMounts(ms []corev1.VolumeMount) []*runtimev1.VolumeMount {
	out := make([]*runtimev1.VolumeMount, 0, len(ms))
	for _, m := range ms {
		out = append(out, &runtimev1.VolumeMount{Name: m.Name, MountPath: m.MountPath, ReadOnly: m.ReadOnly, SubPath: m.SubPath})
	}
	return out
}

func deref32(v *int32) int32 {
	if v == nil {
		return 0
	}
	return *v
}

func derefBool(v *bool) bool { return v != nil && *v }
