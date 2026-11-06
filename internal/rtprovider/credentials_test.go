package rtprovider

import (
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/google/go-containerregistry/pkg/name"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	runtimev1 "k3sm.io/apis/runtime/v1"
)

func TestMatchRegistryKey(t *testing.T) {
	for _, tc := range []struct {
		key, ref string
		match    bool
	}{
		{"ghcr.io", "ghcr.io/org/app:1", true},
		{"https://ghcr.io/org", "ghcr.io/org/app:1", true},
		{"ghcr.io/org", "ghcr.io/organization/app:1", false},
		{"ghcr.io/other", "ghcr.io/org/app:1", false},
		{"https://index.docker.io/v1/", "busybox", true},
		{"docker.io", "library/busybox", true},
		{"*.example.com", "reg.example.com/app", true},
		{"*.example.com", "example.com/app", false},
		{"reg.example.com:5000", "reg.example.com:5000/app", true},
		{"reg.example.com:5000", "reg.example.com/app", false},
		{"reg.example.com", "reg.example.com:5000/app", false},
	} {
		r, err := name.ParseReference(tc.ref)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := matchRegistryKey(tc.key, r.Context()); ok != tc.match {
			t.Errorf("key %q ref %q: match %v, want %v", tc.key, tc.ref, ok, tc.match)
		}
	}
}

func TestPullCredential(t *testing.T) {
	secret := func(name, cfg string) *corev1.Secret {
		return &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: name},
			Type:       corev1.SecretTypeDockerConfigJson,
			Data:       map[string][]byte{corev1.DockerConfigJsonKey: []byte(cfg)},
		}
	}
	client := fake.NewClientset(
		secret("host", `{"auths":{"ghcr.io":{"username":"any","password":"p1"}}}`),
		secret("repo", `{"auths":{"ghcr.io/org":{"username":"org","password":"p2"}}}`),
		secret("broken", `{"auths":`),
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "opaque"}, Type: corev1.SecretTypeOpaque},
	)
	s := secretCredentials{client: client, log: slog.New(slog.DiscardHandler)}
	refs := func(names ...string) (out []*runtimev1.LocalObjectReference) {
		for _, n := range names {
			out = append(out, &runtimev1.LocalObjectReference{Name: n})
		}
		return out
	}
	ctx := context.Background()

	cred, ok, err := s.PullCredential(ctx, "ns", refs("missing", "host", "repo"), "ghcr.io/org/app:1")
	if err != nil || !ok || cred.Username != "org" {
		t.Errorf("most specific match: %+v %v %v", cred, ok, err)
	}
	if cred, ok, _ = s.PullCredential(ctx, "ns", refs("host", "repo"), "ghcr.io/else/app:1"); !ok || cred.Username != "any" {
		t.Errorf("host-wide match: %+v %v", cred, ok)
	}
	if _, ok, err = s.PullCredential(ctx, "ns", refs("host"), "quay.io/app:1"); ok || err != nil {
		t.Errorf("other registry should be anonymous: %v %v", ok, err)
	}
	for _, bad := range []string{"broken", "opaque"} {
		_, _, err = s.PullCredential(ctx, "ns", refs(bad), "ghcr.io/org/app:1")
		if err == nil || strings.Contains(err.Error(), "auths") {
			t.Errorf("%s: want an error that does not quote the Secret, got %v", bad, err)
		}
	}
}

func TestWaitingReason(t *testing.T) {
	pod := testPod(corev1.RestartPolicyAlways, corev1.Container{Name: "c"})
	waiting := func(fr runtimev1.FailureReason) *runtimev1.PodStatus {
		return &runtimev1.PodStatus{ContainerStatuses: []*runtimev1.ContainerStatus{{
			Name: "c", State: &runtimev1.ContainerState{Waiting: &runtimev1.ContainerStateWaiting{FailureReason: fr, Message: "m"}},
		}}}
	}
	for _, tc := range []struct {
		fr      runtimev1.FailureReason
		pulling bool
		want    string
	}{
		{runtimev1.FailureReason_FAILURE_REASON_IMAGE_PULL, false, "ErrImagePull"},
		{runtimev1.FailureReason_FAILURE_REASON_IMAGE_PULL, true, "ImagePullBackOff"},
		{runtimev1.FailureReason_FAILURE_REASON_IMAGE_PULL_CREDENTIAL, true, "ImagePullBackOff"},
		{runtimev1.FailureReason_FAILURE_REASON_INVALID_IMAGE_NAME, false, "InvalidImageName"},
		{runtimev1.FailureReason_FAILURE_REASON_IMAGE_NEVER_PULL, true, "ErrImageNeverPull"},
	} {
		got := toPodStatus(pod, waiting(tc.fr), "ip", overrides{pulling: map[string]bool{"c": tc.pulling}})
		w := got.ContainerStatuses[0].State.Waiting
		if w == nil || w.Reason != tc.want || got.Phase != corev1.PodPending {
			t.Errorf("%s pulling=%v: %+v phase %s, want %s/Pending", tc.fr, tc.pulling, w, got.Phase, tc.want)
		}
	}
	if retryable(runtimev1.FailureReason_FAILURE_REASON_INVALID_IMAGE_NAME) || !retryable(runtimev1.FailureReason_FAILURE_REASON_IMAGE_PULL) {
		t.Error("retryable classification wrong")
	}
}
