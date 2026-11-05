package main

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/node/api"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func testPod(name string, policy v1.RestartPolicy, init []v1.Container, ctrs ...v1.Container) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: name, UID: types.UID("uid-" + name)},
		Spec:       v1.PodSpec{RestartPolicy: policy, InitContainers: init, Containers: ctrs},
	}
}

func waitPhase(t *testing.T, p *Provider, name string, want v1.PodPhase) *v1.PodStatus {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		st, err := p.GetPodStatus(context.Background(), "default", name)
		if err != nil {
			t.Fatal(err)
		}
		if st.Phase == want {
			return st
		}
		if time.Now().After(deadline) {
			t.Fatalf("phase %s, want %s", st.Phase, want)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestPodLifecycle(t *testing.T) {
	ctx := context.Background()
	p, _ := NewProvider("mac", "10.0.0.5", t.TempDir(), false)

	pod := testPod("job", v1.RestartPolicyNever,
		[]v1.Container{{Name: "init", Command: []string{"sh", "-c", "echo init"}}},
		v1.Container{Name: "main", Command: []string{"sh", "-c"}, Args: []string{"echo hello $FOO; echo bye"},
			Env: []v1.EnvVar{{Name: "FOO", Value: "darwin"}}})
	if err := p.CreatePod(ctx, pod); err != nil {
		t.Fatal(err)
	}
	st := waitPhase(t, p, "job", v1.PodSucceeded)
	if st.PodIP != "10.0.0.5" {
		t.Errorf("podIP %q", st.PodIP)
	}
	rc, err := p.GetContainerLogs(ctx, "default", "job", "main", api.ContainerLogOpts{Tail: 1})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(rc)
	rc.Close()
	if string(b) != "bye\n" {
		t.Errorf("tail log %q", b)
	}
	rc, _ = p.GetContainerLogs(ctx, "default", "job", "main", api.ContainerLogOpts{})
	b, _ = io.ReadAll(rc)
	rc.Close()
	if !strings.Contains(string(b), "hello darwin") {
		t.Errorf("log %q", b)
	}

	fail := testPod("bad", v1.RestartPolicyNever, nil, v1.Container{Name: "c", Command: []string{"false"}})
	_ = p.CreatePod(ctx, fail)
	waitPhase(t, p, "bad", v1.PodFailed)

	svc := testPod("svc", v1.RestartPolicyAlways, nil, v1.Container{Name: "c", Command: []string{"sleep", "60"}})
	_ = p.CreatePod(ctx, svc)
	st = waitPhase(t, p, "svc", v1.PodRunning)
	pid := 0
	if st.ContainerStatuses[0].State.Running == nil {
		t.Fatal("not running")
	}
	pr, _ := p.get("default", "svc")
	pr.mu.Lock()
	pid = pr.containers["c"].proc.Pid
	pr.mu.Unlock()
	grace := int64(1)
	svc.Spec.TerminationGracePeriodSeconds = &grace
	if err := p.DeletePod(ctx, svc); err != nil {
		t.Fatal(err)
	}
	if _, err := p.GetPod(ctx, "default", "svc"); err == nil {
		t.Error("pod still present after delete")
	}
	time.Sleep(500 * time.Millisecond)
	pr.mu.Lock()
	terminated := pr.containers["c"].state.Terminated != nil
	pr.mu.Unlock()
	if !terminated {
		t.Errorf("process %d not terminated", pid)
	}
}
