package main

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/node"
	"golang.org/x/sys/unix"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// diskPressureFree is the free space on the data volume below which the node
// reports DiskPressure. runtimed's image GC starts reclaiming at the same mark.
const diskPressureFree = 5 << 30

// hostSample is what the node conditions are computed from.
type hostSample struct {
	vmPressureLevel   uint32 // kern.memorystatus_vm_pressure_level: 1 normal, 2 warning, 4 critical
	diskFree          uint64 // bytes free on the data volume
	procs, maxProcs   int
	sampledDisk, pids bool
}

// nodeConditions maps a sample to the node's Ready and pressure conditions.
// MemoryPressure fires only at macOS's critical level: warning is routine on
// a Mac in daily use, and a True MemoryPressure taints the node against new
// best-effort pods.
func nodeConditions(s hostSample) []v1.NodeCondition {
	cond := func(t v1.NodeConditionType, on bool, reason, msg string) v1.NodeCondition {
		c := v1.NodeCondition{Type: t, Status: v1.ConditionFalse, Reason: "KubeletHasSufficient" + strings.TrimSuffix(string(t), "Pressure"), Message: "no " + strings.ToLower(string(t))}
		if on {
			c.Status, c.Reason, c.Message = v1.ConditionTrue, reason, msg
		}
		return c
	}
	return []v1.NodeCondition{
		{Type: v1.NodeReady, Status: v1.ConditionTrue, Reason: "KubeletReady", Message: "darwin-vk is ready"},
		cond(v1.NodeMemoryPressure, s.vmPressureLevel >= 4, "KubeletHasInsufficientMemory", "macOS reports critical memory pressure"),
		cond(v1.NodeDiskPressure, s.sampledDisk && s.diskFree < diskPressureFree, "KubeletHasDiskPressure",
			fmt.Sprintf("%s free on the data volume", resource.NewQuantity(int64(s.diskFree), resource.BinarySI))),
		cond(v1.NodePIDPressure, s.pids && s.maxProcs > 0 && s.procs*10 > s.maxProcs*9, "KubeletHasInsufficientPID",
			fmt.Sprintf("%d of %d processes in use", s.procs, s.maxProcs)),
	}
}

func sampleHost(dataDir string) hostSample {
	var s hostSample
	s.vmPressureLevel, _ = unix.SysctlUint32("kern.memorystatus_vm_pressure_level")
	var fs unix.Statfs_t
	if unix.Statfs(dataDir, &fs) == nil {
		s.diskFree, s.sampledDisk = fs.Bavail*uint64(fs.Bsize), true
	}
	if max, err := unix.SysctlUint32("kern.maxproc"); err == nil {
		if procs, err := unix.SysctlKinfoProcSlice("kern.proc.all"); err == nil {
			s.procs, s.maxProcs, s.pids = len(procs), int(max), true
		}
	}
	return s
}

// nodeStatus keeps the node's conditions current. Supplying it replaces
// virtual-kubelet's default node provider, which only ever marks Ready.
type nodeStatus struct {
	*node.NaiveNodeProviderV2
	log     *slog.Logger
	dataDir string

	mu sync.Mutex // guards n
	n  *v1.Node
}

func newNodeStatus(n *v1.Node, dataDir string, log *slog.Logger) *nodeStatus {
	s := &nodeStatus{NaiveNodeProviderV2: node.NewNaiveNodeProvider(), log: log, dataDir: dataDir, n: n}
	setConditions(n, nodeConditions(sampleHost(dataDir)))
	n.Status.Phase = v1.NodeRunning
	return s
}

// NotifyNodeStatus registers virtual-kubelet's callback and starts sampling.
func (s *nodeStatus) NotifyNodeStatus(ctx context.Context, cb func(*v1.Node)) {
	s.NaiveNodeProviderV2.NotifyNodeStatus(ctx, cb)
	go s.run(ctx, 10*time.Second)
}

func (s *nodeStatus) run(ctx context.Context, every time.Duration) {
	tick := time.NewTicker(every)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		s.mu.Lock()
		changed := setConditions(s.n, nodeConditions(sampleHost(s.dataDir)))
		n := s.n.DeepCopy()
		s.mu.Unlock()
		if !changed {
			continue
		}
		s.log.Info("node conditions changed", "conditions", conditionSummary(n.Status.Conditions))
		if err := s.UpdateStatus(ctx, n); err != nil {
			s.log.Warn("update node status", "err", err)
		}
	}
}

// setConditions replaces the node's conditions of the same types, keeping
// transition times for those whose status did not change, and reports whether
// any did.
func setConditions(n *v1.Node, next []v1.NodeCondition) bool {
	now := metav1.Now()
	changed := false
	for _, c := range next {
		c.LastHeartbeatTime = now
		i := -1
		for j := range n.Status.Conditions {
			if n.Status.Conditions[j].Type == c.Type {
				i = j
			}
		}
		switch {
		case i < 0:
			c.LastTransitionTime = now
			n.Status.Conditions = append(n.Status.Conditions, c)
			changed = true
		case n.Status.Conditions[i].Status != c.Status:
			c.LastTransitionTime = now
			n.Status.Conditions[i] = c
			changed = true
		default:
			c.LastTransitionTime = n.Status.Conditions[i].LastTransitionTime
			n.Status.Conditions[i] = c
		}
	}
	return changed
}

func conditionSummary(cs []v1.NodeCondition) string {
	parts := make([]string, 0, len(cs))
	for _, c := range cs {
		parts = append(parts, fmt.Sprintf("%s=%s", c.Type, c.Status))
	}
	return strings.Join(parts, " ")
}

// parseReserved parses --system-reserved ("cpu=1,memory=2Gi").
func parseReserved(s string) (v1.ResourceList, error) {
	out := v1.ResourceList{}
	for _, kv := range strings.Split(s, ",") {
		if kv = strings.TrimSpace(kv); kv == "" {
			continue
		}
		k, val, ok := strings.Cut(kv, "=")
		if !ok || (k != "cpu" && k != "memory") {
			return nil, fmt.Errorf("--system-reserved: want cpu=<q>,memory=<q>, got %q", kv)
		}
		q, err := resource.ParseQuantity(val)
		if err != nil {
			return nil, fmt.Errorf("--system-reserved %s: %w", k, err)
		}
		out[v1.ResourceName(k)] = q
	}
	return out, nil
}
