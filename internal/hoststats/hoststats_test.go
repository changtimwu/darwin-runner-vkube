package hoststats

import (
	"runtime"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestRead(t *testing.T) {
	var r Reader
	a, err := r.Read()
	if err != nil {
		t.Fatal(err)
	}
	if a.CPUCoreNanoSeconds == 0 {
		t.Error("cumulative CPU is 0")
	}
	if a.CPUNanoCores != 0 {
		t.Errorf("first read rate = %d, want 0", a.CPUNanoCores)
	}
	mem, _ := unix.SysctlUint64("hw.memsize")
	if a.MemoryWorkingSetBytes == 0 || a.MemoryWorkingSetBytes > mem {
		t.Errorf("working set %d not in (0, %d]", a.MemoryWorkingSetBytes, mem)
	}

	// Burn some CPU so the second read sees a non-zero, plausible rate.
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
	}
	b, err := r.Read()
	if err != nil {
		t.Fatal(err)
	}
	if b.CPUCoreNanoSeconds < a.CPUCoreNanoSeconds {
		t.Errorf("cumulative CPU went backwards: %d -> %d", a.CPUCoreNanoSeconds, b.CPUCoreNanoSeconds)
	}
	if max := uint64(runtime.NumCPU()) * uint64(time.Second); b.CPUNanoCores == 0 || b.CPUNanoCores > max {
		t.Errorf("rate %d not in (0, %d]", b.CPUNanoCores, max)
	}
}
