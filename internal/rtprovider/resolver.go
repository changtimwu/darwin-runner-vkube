package rtprovider

import (
	"context"
	"errors"
	"fmt"
	"os"

	authnv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"k3sm.io/runtimed/pkg/mount"
)

// kubeResolver feeds runtimed's volume materializer: ConfigMap and Secret
// data, and service account tokens bound to the pod. runtimed writes the
// files; it never talks to the API server itself.
type kubeResolver struct {
	client kubernetes.Interface
}

// ConfigMap returns the ConfigMap's data and binaryData merged.
func (k kubeResolver) ConfigMap(ctx context.Context, namespace, name string) (mount.SourceData, error) {
	cm, err := k.client.CoreV1().ConfigMaps(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return mount.SourceData{}, notExist("configmap", namespace, name, err)
	}
	data := make(map[string][]byte, len(cm.Data)+len(cm.BinaryData))
	for k, v := range cm.Data {
		data[k] = []byte(v)
	}
	for k, v := range cm.BinaryData {
		data[k] = v
	}
	return mount.SourceData{Data: data, Immutable: cm.Immutable != nil && *cm.Immutable}, nil
}

// Secret returns the Secret's data.
func (k kubeResolver) Secret(ctx context.Context, namespace, name string) (mount.SourceData, error) {
	s, err := k.client.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return mount.SourceData{}, notExist("secret", namespace, name, err)
	}
	return mount.SourceData{Data: s.Data, Immutable: s.Immutable != nil && *s.Immutable}, nil
}

// ServiceAccountToken mints a token for the pod's service account, bound to
// the pod so it dies with it. The pod comes from ctx (withPod), because
// runtimed's seam carries only the namespace.
func (k kubeResolver) ServiceAccountToken(ctx context.Context, namespace, audience string, expirationSeconds int64) (string, error) {
	pod, ok := ctx.Value(podKey{}).(*corev1.Pod)
	if !ok {
		return "", errors.New("service account token requested outside a pod operation")
	}
	sa := pod.Spec.ServiceAccountName
	if sa == "" {
		sa = "default"
	}
	if expirationSeconds <= 0 {
		expirationSeconds = 3600
	}
	req := &authnv1.TokenRequest{Spec: authnv1.TokenRequestSpec{
		ExpirationSeconds: &expirationSeconds,
		BoundObjectRef:    &authnv1.BoundObjectReference{Kind: "Pod", APIVersion: "v1", Name: pod.Name, UID: pod.UID},
	}}
	if audience != "" {
		req.Spec.Audiences = []string{audience}
	}
	tr, err := k.client.CoreV1().ServiceAccounts(namespace).CreateToken(ctx, sa, req, metav1.CreateOptions{})
	if err != nil {
		return "", fmt.Errorf("token for serviceaccount %s/%s: %w", namespace, sa, err)
	}
	return tr.Status.Token, nil
}

// podKey carries the pod a runtimed call acts for, for ServiceAccountToken.
type podKey struct{}

func withPod(ctx context.Context, pod *corev1.Pod) context.Context {
	return context.WithValue(ctx, podKey{}, pod)
}

// notExist maps NotFound to os.ErrNotExist, which runtimed reads as "skip an
// optional source"; any other error fails the volume.
func notExist(kind, namespace, name string, err error) error {
	if apierrors.IsNotFound(err) {
		return fmt.Errorf("%s %s/%s: %w", kind, namespace, name, os.ErrNotExist)
	}
	return fmt.Errorf("get %s %s/%s: %w", kind, namespace, name, err)
}
