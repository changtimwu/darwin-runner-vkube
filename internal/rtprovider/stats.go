package rtprovider

import (
	"context"
	"time"

	dto "github.com/prometheus/client_model/go"
	"google.golang.org/protobuf/proto"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	statsv1alpha1 "k8s.io/kubelet/pkg/apis/stats/v1alpha1"

	runtimev1 "k3sm.io/apis/runtime/v1"
)

// GetStatsSummary serves /stats/summary from runtimed's per-pod accounting
// (proc_pid_rusage), so `kubectl top` works through metrics-server.
func (p *Provider) GetStatsSummary(ctx context.Context) (*statsv1alpha1.Summary, error) {
	resp, err := p.rt.ListPodStats(ctx, &runtimev1.ListPodStatsRequest{})
	if err != nil {
		return nil, err
	}
	now := metav1.Now()
	sum := &statsv1alpha1.Summary{Node: statsv1alpha1.NodeStats{NodeName: p.cfg.NodeName, StartTime: now}}
	for _, ps := range resp.GetPodStats() {
		p.mu.Lock()
		t, ok := p.pods[ps.GetPodId()]
		var ref statsv1alpha1.PodReference
		if ok {
			ref = statsv1alpha1.PodReference{Name: t.pod.Name, Namespace: t.pod.Namespace, UID: string(t.pod.UID)}
		}
		p.mu.Unlock()
		if !ok {
			continue
		}
		pod := statsv1alpha1.PodStats{PodRef: ref, StartTime: now, CPU: cpuStats(ps.GetCpu()), Memory: memStats(ps.GetMemory())}
		for _, cs := range ps.GetContainers() {
			pod.Containers = append(pod.Containers, statsv1alpha1.ContainerStats{
				Name: cs.GetName(), StartTime: now, CPU: cpuStats(cs.GetCpu()), Memory: memStats(cs.GetMemory()),
			})
		}
		sum.Pods = append(sum.Pods, pod)
	}
	// Node figures are host-wide, as the kubelet reports them. metrics-server
	// drops a node whose CPU or memory reads 0, so pod sums are not enough.
	h, err := p.host.Read()
	if err != nil {
		return nil, err
	}
	ht := metav1.Time{Time: h.Time}
	sum.Node.CPU = &statsv1alpha1.CPUStats{Time: ht, UsageNanoCores: &h.CPUNanoCores, UsageCoreNanoSeconds: &h.CPUCoreNanoSeconds}
	sum.Node.Memory = &statsv1alpha1.MemoryStats{Time: ht, WorkingSetBytes: &h.MemoryWorkingSetBytes}
	return sum, nil
}

func cpuStats(c *runtimev1.CPUStats) *statsv1alpha1.CPUStats {
	if c == nil {
		return nil
	}
	return &statsv1alpha1.CPUStats{
		Time:                 stamp(c.GetTimestamp().AsTime()),
		UsageNanoCores:       proto.Uint64(c.GetUsageNanoCores()),
		UsageCoreNanoSeconds: proto.Uint64(c.GetUsageCoreNanoSeconds()),
	}
}

func memStats(m *runtimev1.MemoryStats) *statsv1alpha1.MemoryStats {
	if m == nil {
		return nil
	}
	return &statsv1alpha1.MemoryStats{
		Time:            stamp(m.GetTimestamp().AsTime()),
		WorkingSetBytes: proto.Uint64(m.GetWorkingSetBytes()),
		UsageBytes:      proto.Uint64(m.GetUsageBytes()),
		RSSBytes:        proto.Uint64(m.GetRssBytes()),
	}
}

func stamp(t time.Time) metav1.Time {
	if t.IsZero() || t.Unix() == 0 {
		return metav1.Now()
	}
	return metav1.Time{Time: t}
}

// GetMetricsResource serves /metrics/resource, the kubelet resource-metrics
// families metrics-server scrapes for `kubectl top` and the HPA.
func (p *Provider) GetMetricsResource(ctx context.Context) ([]*dto.MetricFamily, error) {
	sum, err := p.GetStatsSummary(ctx)
	if err != nil {
		return nil, err
	}
	nowMs := time.Now().UnixMilli()
	fam := func(name, help string, typ dto.MetricType) *dto.MetricFamily {
		return &dto.MetricFamily{Name: proto.String(name), Help: proto.String(help), Type: typ.Enum()}
	}
	add := func(f *dto.MetricFamily, v float64, labels ...string) {
		m := &dto.Metric{TimestampMs: proto.Int64(nowMs)}
		for i := 0; i+1 < len(labels); i += 2 {
			m.Label = append(m.Label, &dto.LabelPair{Name: proto.String(labels[i]), Value: proto.String(labels[i+1])})
		}
		if f.GetType() == dto.MetricType_COUNTER {
			m.Counter = &dto.Counter{Value: proto.Float64(v)}
		} else {
			m.Gauge = &dto.Gauge{Value: proto.Float64(v)}
		}
		f.Metric = append(f.Metric, m)
	}
	nodeCPU := fam("node_cpu_usage_seconds_total", "Cumulative cpu time consumed by the node in core-seconds", dto.MetricType_COUNTER)
	nodeMem := fam("node_memory_working_set_bytes", "Current working set of the node in bytes", dto.MetricType_GAUGE)
	podCPU := fam("pod_cpu_usage_seconds_total", "Cumulative cpu time consumed by the pod in core-seconds", dto.MetricType_COUNTER)
	podMem := fam("pod_memory_working_set_bytes", "Current working set of the pod in bytes", dto.MetricType_GAUGE)
	ctrCPU := fam("container_cpu_usage_seconds_total", "Cumulative cpu time consumed by the container in core-seconds", dto.MetricType_COUNTER)
	ctrMem := fam("container_memory_working_set_bytes", "Current working set of the container in bytes", dto.MetricType_GAUGE)
	add(nodeCPU, float64(*sum.Node.CPU.UsageCoreNanoSeconds)/1e9)
	add(nodeMem, float64(*sum.Node.Memory.WorkingSetBytes))
	for _, ps := range sum.Pods {
		// CPU and memory are published jointly or not at all: metrics-server
		// drops a record that lacks either half.
		if ps.CPU == nil || ps.Memory == nil {
			continue
		}
		ns, name := ps.PodRef.Namespace, ps.PodRef.Name
		add(podCPU, float64(*ps.CPU.UsageCoreNanoSeconds)/1e9, "namespace", ns, "pod", name)
		add(podMem, float64(*ps.Memory.WorkingSetBytes), "namespace", ns, "pod", name)
		for _, cs := range ps.Containers {
			if cs.CPU == nil || cs.Memory == nil {
				continue
			}
			add(ctrCPU, float64(*cs.CPU.UsageCoreNanoSeconds)/1e9, "container", cs.Name, "namespace", ns, "pod", name)
			add(ctrMem, float64(*cs.Memory.WorkingSetBytes), "container", cs.Name, "namespace", ns, "pod", name)
		}
	}
	// The text encoder rejects a family with no samples, which on a node
	// without running pods would fail the whole scrape.
	var out []*dto.MetricFamily
	for _, f := range []*dto.MetricFamily{nodeCPU, nodeMem, podCPU, podMem, ctrCPU, ctrMem} {
		if len(f.Metric) > 0 {
			out = append(out, f)
		}
	}
	return out, nil
}
