// Package hoststats reads host-wide CPU and memory usage from Mach, for the
// node figures in /stats/summary and /metrics/resource.
package hoststats

/*
#include <mach/mach.h>
#include <unistd.h>

static kern_return_t cpu_ticks(natural_t *busy, natural_t *total) {
	host_cpu_load_info_data_t info;
	mach_msg_type_number_t n = HOST_CPU_LOAD_INFO_COUNT;
	kern_return_t kr = host_statistics(mach_host_self(), HOST_CPU_LOAD_INFO, (host_info_t)&info, &n);
	if (kr != KERN_SUCCESS) return kr;
	*busy = info.cpu_ticks[CPU_STATE_USER] + info.cpu_ticks[CPU_STATE_SYSTEM] + info.cpu_ticks[CPU_STATE_NICE];
	*total = *busy + info.cpu_ticks[CPU_STATE_IDLE];
	return KERN_SUCCESS;
}

static kern_return_t vm_stats(vm_statistics64_data_t *vs) {
	mach_msg_type_number_t n = HOST_VM_INFO64_COUNT;
	return host_statistics64(mach_host_self(), HOST_VM_INFO64, (host_info64_t)vs, &n);
}

static long clk_tck(void) { return sysconf(_SC_CLK_TCK); }
*/
import "C"

import (
	"fmt"
	"sync"
	"time"
)

// Sample is one reading of host usage.
type Sample struct {
	Time time.Time
	// CPUCoreNanoSeconds is cumulative busy CPU time across all cores since boot.
	CPUCoreNanoSeconds uint64
	// CPUNanoCores is the busy rate since the previous Read, 0 on the first.
	CPUNanoCores uint64
	// MemoryWorkingSetBytes matches Activity Monitor's "Memory Used":
	// app (anonymous, non-purgeable) + wired + compressed pages.
	MemoryWorkingSetBytes uint64
}

// Reader keeps the previous CPU reading so it can report a rate.
type Reader struct {
	mu       sync.Mutex
	prevTime time.Time
	prevCPU  uint64
}

// Read samples the host.
func (r *Reader) Read() (Sample, error) {
	var busy, total C.natural_t
	if kr := C.cpu_ticks(&busy, &total); kr != C.KERN_SUCCESS {
		return Sample{}, fmt.Errorf("host_statistics(HOST_CPU_LOAD_INFO): kern_return %d", kr)
	}
	var vs C.vm_statistics64_data_t
	if kr := C.vm_stats(&vs); kr != C.KERN_SUCCESS {
		return Sample{}, fmt.Errorf("host_statistics64(HOST_VM_INFO64): kern_return %d", kr)
	}
	now := time.Now()
	s := Sample{
		Time:               now,
		CPUCoreNanoSeconds: uint64(busy) * uint64(time.Second) / uint64(C.clk_tck()),
	}
	pages := uint64(vs.internal_page_count) - uint64(vs.purgeable_count) +
		uint64(vs.wire_count) + uint64(vs.compressor_page_count)
	s.MemoryWorkingSetBytes = pages * uint64(C.vm_kernel_page_size)

	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.prevTime.IsZero() && s.CPUCoreNanoSeconds >= r.prevCPU {
		if dt := now.Sub(r.prevTime); dt > 0 {
			s.CPUNanoCores = uint64(float64(s.CPUCoreNanoSeconds-r.prevCPU) / dt.Seconds())
		}
	}
	r.prevTime, r.prevCPU = now, s.CPUCoreNanoSeconds
	return s, nil
}
