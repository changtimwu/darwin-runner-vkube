package rtprovider

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	authnv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestToVolumes(t *testing.T) {
	mode := int32(0o400)
	vs, err := toVolumes([]corev1.Volume{
		{Name: "cm", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
			LocalObjectReference: corev1.LocalObjectReference{Name: "c"}, DefaultMode: &mode,
			Items: []corev1.KeyToPath{{Key: "k", Path: "p"}}}}},
		{Name: "tok", VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{Sources: []corev1.VolumeProjection{
			{ServiceAccountToken: &corev1.ServiceAccountTokenProjection{Path: "token"}},
			{DownwardAPI: &corev1.DownwardAPIProjection{Items: []corev1.DownwardAPIVolumeFile{{Path: "ns", FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}}}},
		}}}},
		{Name: "tmp", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if cm := vs[0].GetConfigMap(); cm.GetName() != "c" || cm.GetDefaultMode() != 0o400 || cm.GetItems()[0].GetPath() != "p" {
		t.Errorf("configMap: %v", cm)
	}
	if src := vs[1].GetProjected().GetSources(); src[0].GetServiceAccountToken().GetPath() != "token" ||
		src[1].GetDownwardApi().GetItems()[0].GetFieldRef().GetFieldPath() != "metadata.namespace" {
		t.Errorf("projected: %v", src)
	}

	for name, src := range map[string]corev1.VolumeSource{
		"hostPath":     {HostPath: &corev1.HostPathVolumeSource{Path: "/"}},
		"pvc":          {PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "x"}},
		"nfs":          {NFS: &corev1.NFSVolumeSource{}},
		"whole labels": {DownwardAPI: &corev1.DownwardAPIVolumeSource{Items: []corev1.DownwardAPIVolumeFile{{Path: "l", FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.labels"}}}}},
	} {
		if _, err := toVolumes([]corev1.Volume{{Name: "v", VolumeSource: src}}); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
}

func TestResolver(t *testing.T) {
	immutable := true
	client := fake.NewClientset(
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "c"}, Data: map[string]string{"a": "1"},
			BinaryData: map[string][]byte{"b": {2}}, Immutable: &immutable},
	)
	var got *authnv1.TokenRequest
	client.PrependReactor("create", "serviceaccounts", func(a k8stesting.Action) (bool, runtime.Object, error) {
		got = a.(k8stesting.CreateAction).GetObject().(*authnv1.TokenRequest)
		return true, &authnv1.TokenRequest{Status: authnv1.TokenRequestStatus{Token: "jwt"}}, nil
	})
	r := kubeResolver{client: client}
	ctx := context.Background()

	d, err := r.ConfigMap(ctx, "ns", "c")
	if err != nil || string(d.Data["a"]) != "1" || d.Data["b"][0] != 2 || !d.Immutable {
		t.Errorf("configmap: %+v %v", d, err)
	}
	if _, err := r.Secret(ctx, "ns", "missing"); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("missing secret should wrap os.ErrNotExist: %v", err)
	}
	if _, err := r.ServiceAccountToken(ctx, "ns", "", 0); err == nil {
		t.Error("token minted without a pod")
	}
	pod := testPod("", corev1.Container{Name: "c"})
	pod.Spec.ServiceAccountName = "builder"
	tok, err := r.ServiceAccountToken(withPod(ctx, pod), "ns", "vault", 0)
	if err != nil || tok != "jwt" {
		t.Fatalf("token: %q %v", tok, err)
	}
	if ref := got.Spec.BoundObjectRef; ref.Name != pod.Name || ref.UID != pod.UID || got.Spec.Audiences[0] != "vault" || *got.Spec.ExpirationSeconds != 3600 {
		t.Errorf("token request: %+v", got.Spec)
	}
}

func TestExecCommand(t *testing.T) {
	root := t.TempDir()
	p := &Provider{cfg: Config{Root: root}}
	pod := testPod("", corev1.Container{Name: "c"})
	bin := root + "/pods/" + string(pod.UID) + "/rootfs/bin"
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(bin+"/tool", nil, 0o755); err != nil {
		t.Fatal(err)
	}
	if got := p.execCommand(pod, []string{"/bin/tool", "-v"}); !strings.HasSuffix(got[0], "/rootfs/bin/tool") || got[1] != "-v" {
		t.Errorf("image binary not resolved in rootfs: %v", got)
	}
	for _, cmd := range [][]string{{"/bin/sh"}, {"sh"}, nil} {
		if got := p.execCommand(pod, cmd); len(got) != len(cmd) || (len(cmd) > 0 && got[0] != cmd[0]) {
			t.Errorf("%v rewritten to %v", cmd, got)
		}
	}
}
