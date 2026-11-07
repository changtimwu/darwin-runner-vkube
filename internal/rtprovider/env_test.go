package rtprovider

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestContainerEnv(t *testing.T) {
	yes := true
	client := fake.NewSimpleClientset(
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "cm"},
			Data: map[string]string{"MODE": "fast", "bad=key": "x", "LEVEL": "3"}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "sec"},
			Data: map[string][]byte{"PASSWORD": []byte("hunter2")}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "kubernetes"},
			Spec: corev1.ServiceSpec{ClusterIP: "10.43.0.1", Ports: []corev1.ServicePort{{Name: "https", Port: 443, Protocol: corev1.ProtocolTCP}}}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "my-db"},
			Spec: corev1.ServiceSpec{ClusterIP: "10.43.9.9", Ports: []corev1.ServicePort{{Port: 5432}}}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "mac"},
			Status: corev1.NodeStatus{Allocatable: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("30Gi")}}},
	)
	p := &Provider{cfg: Config{NodeName: "mac", NodeIP: "172.16.2.143", Root: "/r", Client: client}}
	pod := testPod(corev1.RestartPolicyAlways, corev1.Container{
		Name: "app", Image: "native", Command: []string{"/bin/true"},
		EnvFrom: []corev1.EnvFromSource{
			{Prefix: "CM_", ConfigMapRef: &corev1.ConfigMapEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "cm"}}},
			{ConfigMapRef: &corev1.ConfigMapEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "absent"}, Optional: &yes}},
		},
		Env: []corev1.EnvVar{
			{Name: "POD_IP", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.podIP"}}},
			{Name: "HOST_IP", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.hostIP"}}},
			{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
			{Name: "TIER", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.labels['tier']"}}},
			{Name: "PASSWORD", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "sec"}, Key: "PASSWORD"}}},
			{Name: "OPTIONAL", ValueFrom: &corev1.EnvVarSource{ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "cm"}, Key: "nope", Optional: &yes}}},
			{Name: "CPU_MILLI", ValueFrom: &corev1.EnvVarSource{ResourceFieldRef: &corev1.ResourceFieldSelector{
				Resource: "limits.cpu", Divisor: resource.MustParse("1m")}}},
			{Name: "MEM_MI", ValueFrom: &corev1.EnvVarSource{ResourceFieldRef: &corev1.ResourceFieldSelector{
				Resource: "limits.memory", Divisor: resource.MustParse("1Mi")}}},
			{Name: "URL", Value: "http://$(POD_IP):8080/$(CM_MODE)?db=$(MY_DB_SERVICE_HOST)&x=$(UNKNOWN)&$$(ESCAPED)"},
		},
		Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1500m")}},
	})
	pod.Labels = map[string]string{"tier": "web"}
	pod.Spec.NodeName = "mac"

	box, err := p.podBoxAt(context.Background(), pod, "10.42.1.7")
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, e := range box.GetContainers()[0].GetEnv() {
		got[e.GetName()] = e.GetValue()
	}
	want := map[string]string{
		"POD_IP":                        "10.42.1.7",
		"HOST_IP":                       "172.16.2.143",
		"POD_NAME":                      "p",
		"TIER":                          "web",
		"PASSWORD":                      "hunter2",
		"CM_MODE":                       "fast",
		"CM_LEVEL":                      "3",
		"CPU_MILLI":                     "1500",
		"MEM_MI":                        "30720", // no limit: node allocatable
		"URL":                           "http://10.42.1.7:8080/fast?db=10.43.9.9&x=$(UNKNOWN)&$(ESCAPED)",
		"KUBERNETES_SERVICE_HOST":       "10.43.0.1",
		"KUBERNETES_SERVICE_PORT":       "443",
		"KUBERNETES_SERVICE_PORT_HTTPS": "443",
		"KUBERNETES_PORT":               "tcp://10.43.0.1:443",
		"KUBERNETES_PORT_443_TCP_ADDR":  "10.43.0.1",
		"MY_DB_SERVICE_HOST":            "10.43.9.9",
		"MY_DB_PORT_5432_TCP":           "tcp://10.43.9.9:5432",
		"MY_DB_PORT_5432_TCP_PROTO":     "tcp",
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %q, want %q", k, got[k], v)
		}
	}
	for _, k := range []string{"OPTIONAL", "CM_bad=key"} {
		if _, ok := got[k]; ok {
			t.Errorf("%s should be unset, got %q", k, got[k])
		}
	}

	// enableServiceLinks: false keeps only the master service.
	no := false
	pod.Spec.EnableServiceLinks = &no
	box, err = p.podBoxAt(context.Background(), pod, "10.42.1.7")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range box.GetContainers()[0].GetEnv() {
		if e.GetName() == "MY_DB_SERVICE_HOST" {
			t.Error("service links added with enableServiceLinks: false")
		}
	}
}

func TestContainerEnvMissingSource(t *testing.T) {
	p := &Provider{cfg: Config{NodeIP: "172.16.2.143", Root: "/r", Client: fake.NewSimpleClientset()}}
	pod := testPod(corev1.RestartPolicyAlways, corev1.Container{
		Name: "app", Image: "native", Command: []string{"/bin/true"},
		Env: []corev1.EnvVar{
			{Name: "A", Value: "1"},
			{Name: "MODE", ValueFrom: &corev1.EnvVarSource{ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "later"}, Key: "MODE"}}},
		},
	})
	_, err := p.podBoxAt(context.Background(), pod, "10.42.1.7")
	if !errors.As(err, new(envError)) {
		t.Fatalf("missing ConfigMap: err = %v, want an envError (retried, not rejected)", err)
	}

	// Re-attach keeps the pod with what resolves.
	box, err := p.podBoxAt(withLenientEnv(context.Background()), pod, "10.42.1.7")
	if err != nil {
		t.Fatal(err)
	}
	if env := box.GetContainers()[0].GetEnv(); len(env) != 1 || env[0].GetName() != "A" {
		t.Errorf("lenient env = %v, want only A", env)
	}
}

func TestExpand(t *testing.T) {
	vars := map[string]string{"A": "x", "B": "y"}
	lookup := func(k string) (string, bool) { v, ok := vars[k]; return v, ok }
	for in, want := range map[string]string{
		"plain":     "plain",
		"$(A)-$(B)": "x-y",
		"$$(A)":     "$(A)",
		"$(C)":      "$(C)",
		"$(A":       "$(A",
		"cost $5 $": "cost $5 $",
		"$()":       "$()",
		"$$$(A)":    "$x",
	} {
		if got := expand(in, lookup); got != want {
			t.Errorf("expand(%q) = %q, want %q", in, got, want)
		}
	}
}

// recordingNetwork hands out one IP per pod and records releases.
type recordingNetwork struct {
	PodNetwork
	live map[string]bool
}

func (n *recordingNetwork) Setup(_ context.Context, podID string) (string, error) {
	n.live[podID] = true
	return "10.42.1.7", nil
}

func (n *recordingNetwork) Teardown(podID string) error {
	delete(n.live, podID)
	return nil
}

func TestCreatePodReleasesIPWhileEnvIsMissing(t *testing.T) {
	net := &recordingNetwork{live: map[string]bool{}}
	p := &Provider{
		cfg: Config{NodeName: "mac", NodeIP: "172.16.2.143", Root: t.TempDir(), Client: fake.NewSimpleClientset(), Network: net},
		log: slog.New(slog.DiscardHandler),
	}
	pod := testPod(corev1.RestartPolicyAlways, corev1.Container{
		Name: "app", Image: "native", Command: []string{"/bin/true"},
		EnvFrom: []corev1.EnvFromSource{{ConfigMapRef: &corev1.ConfigMapEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "later"}}}},
	})
	if err := p.CreatePod(context.Background(), pod); !errors.As(err, new(envError)) {
		t.Fatalf("CreatePod = %v; want a retried env error", err)
	}
	// The pod is untracked until the retry, so DeletePod could not free it.
	if len(net.live) != 0 {
		t.Errorf("IP still held for a Pending pod: %v", net.live)
	}
}
