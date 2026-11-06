package main

import (
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func status(cs []v1.NodeCondition, t v1.NodeConditionType) v1.ConditionStatus {
	for _, c := range cs {
		if c.Type == t {
			return c.Status
		}
	}
	return ""
}

func TestNodeConditions(t *testing.T) {
	calm := hostSample{vmPressureLevel: 2, diskFree: 50 << 30, procs: 500, maxProcs: 8000, sampledDisk: true, pids: true}
	for _, tc := range []struct {
		name string
		s    hostSample
		typ  v1.NodeConditionType
		want v1.ConditionStatus
	}{
		{"ready", calm, v1.NodeReady, v1.ConditionTrue},
		{"warning is not pressure", calm, v1.NodeMemoryPressure, v1.ConditionFalse},
		{"critical is pressure", hostSample{vmPressureLevel: 4}, v1.NodeMemoryPressure, v1.ConditionTrue},
		{"disk ok", calm, v1.NodeDiskPressure, v1.ConditionFalse},
		{"disk low", hostSample{diskFree: 1 << 30, sampledDisk: true}, v1.NodeDiskPressure, v1.ConditionTrue},
		{"disk unsampled", hostSample{}, v1.NodeDiskPressure, v1.ConditionFalse},
		{"pids high", hostSample{procs: 7300, maxProcs: 8000, pids: true}, v1.NodePIDPressure, v1.ConditionTrue},
	} {
		if got := status(nodeConditions(tc.s), tc.typ); got != tc.want {
			t.Errorf("%s: %s=%s, want %s", tc.name, tc.typ, got, tc.want)
		}
	}
}

func TestSetConditions(t *testing.T) {
	n := &v1.Node{}
	if !setConditions(n, nodeConditions(hostSample{})) {
		t.Fatal("first set reported no change")
	}
	before := n.Status.Conditions[1].LastTransitionTime
	if setConditions(n, nodeConditions(hostSample{})) {
		t.Error("same conditions reported a change")
	}
	if n.Status.Conditions[1].LastTransitionTime != before {
		t.Error("transition time moved without a transition")
	}
	if !setConditions(n, nodeConditions(hostSample{vmPressureLevel: 4})) || status(n.Status.Conditions, v1.NodeMemoryPressure) != v1.ConditionTrue {
		t.Error("memory pressure not applied")
	}
	if len(n.Status.Conditions) != 4 {
		t.Errorf("conditions duplicated: %d", len(n.Status.Conditions))
	}
}

func TestSystemReserved(t *testing.T) {
	r, err := parseReserved("cpu=1, memory=2Gi")
	if err != nil || r.Cpu().Value() != 1 || r.Memory().Value() != 2<<30 {
		t.Fatalf("parse: %v %v", r, err)
	}
	for _, bad := range []string{"gpu=1", "memory", "memory=lots"} {
		if _, err := parseReserved(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
	n := &v1.Node{}
	n.Labels = map[string]string{}
	configureNode(n, "10.0.0.1", 10250, 10, false, t.TempDir(), "test", v1.ResourceList{v1.ResourceMemory: resource.MustParse("2Gi"), v1.ResourceCPU: resource.MustParse("1000")}, nil)
	capMem, alloc := n.Status.Capacity[v1.ResourceMemory], n.Status.Allocatable[v1.ResourceMemory]
	if capMem.Value()-alloc.Value() != 2<<30 {
		t.Errorf("memory allocatable %s of %s", alloc.String(), capMem.String())
	}
	if c := n.Status.Allocatable[v1.ResourceCPU]; c.Sign() != 0 {
		t.Errorf("over-reserved cpu should floor at 0, got %s", c.String())
	}
}
