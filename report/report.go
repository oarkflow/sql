package report

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"syscall"
	"time"

	"github.com/oarkflow/json"

	"github.com/shirou/gopsutil/v4/disk"
	"github.com/shirou/gopsutil/v4/mem"
	"github.com/shirou/gopsutil/v4/process"
)

// Monitor wraps a gopsutil Process pointer for the current process
// and holds additional configuration.
type Monitor struct {
	proc     *process.Process
	diskPath string
}

// New returns a new Monitor instance for the current process using the default disk path "/".
func New() *Monitor {
	return NewWithDiskPath("/")
}

// NewWithDiskPath returns a new Monitor instance for the current process using the specified disk path.
func NewWithDiskPath(diskPath string) *Monitor {
	pid := int32(os.Getpid())
	proc, err := process.NewProcess(pid)
	if err != nil {
		log.Fatalf("failed to create process monitor: %v", err)
	}
	return &Monitor{proc: proc, diskPath: diskPath}
}

// Start is a convenience function to create a new monitor and take a starting snapshot.
func Start() *Snapshot {
	return New().Start()
}

// Snapshot holds resource usage information at a given point in time.
type Snapshot struct {
	Timestamp  time.Time               // When the snapshot was taken.
	CPUTime    float64                 // Cumulative CPU time (user + system) in seconds.
	Memory     uint64                  // Memory usage in bytes (RSS) from gopsutil.
	IOCounters *process.IOCountersStat // Disk IO counters for the process.
	Threads    int32                   // Number of threads in the process.
	DiskUsage  *disk.UsageStat         // Disk usage for the specified path.
	SwapUsage  *mem.SwapMemoryStat     // Swap memory usage.
	// GoMemStats holds detailed runtime memory statistics.
	GoMemStats *runtime.MemStats
}

// Snapshot collects a resource usage snapshot.
func (m *Monitor) Snapshot() (*Snapshot, error) {
	t := time.Now()

	cpuTimes, err := m.proc.Times()
	if err != nil {
		return nil, err
	}
	cpuTime := cpuTimes.User + cpuTimes.System

	memInfo, err := m.proc.MemoryInfo()
	if err != nil {
		return nil, err
	}
	// macOS does not support process.IOCounters()
	var ioCounters *process.IOCountersStat
	if runtime.GOOS != "darwin" {
		ioCounters, err = m.proc.IOCounters()
		if err != nil {
			return nil, err
		}
	} else {
		pid := int32(os.Getpid())
		ioCounters, err = GetIOCountersMac(pid, "/")
		fmt.Println(ioCounters)
		if err != nil {
			return nil, err
		}
	}

	threads, err := m.proc.NumThreads()
	if err != nil {
		return nil, err
	}

	path := m.diskPath
	if path == "" {
		path = "/"
	}
	diskUsage, err := disk.Usage(path)
	if err != nil {
		return nil, err
	}

	swapUsage, err := mem.SwapMemory()
	if err != nil {
		return nil, err
	}

	// Collect runtime memory statistics.
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	return &Snapshot{
		Timestamp:  t,
		CPUTime:    cpuTime,
		Memory:     memInfo.RSS,
		IOCounters: ioCounters, // This will be nil on macOS
		Threads:    threads,
		DiskUsage:  diskUsage,
		SwapUsage:  swapUsage,
		GoMemStats: &ms,
	}, nil
}

func GetIOCountersMac(pid int32, diskPath string) (*process.IOCountersStat, error) {
	// Get resource usage for the current process.
	var ru syscall.Rusage
	// Note: syscall.Getrusage only supports RUSAGE_SELF (current process).
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return nil, fmt.Errorf("getrusage failed: %w", err)
	}

	// Retrieve filesystem block size for diskPath.
	var fsStat syscall.Statfs_t
	blockSize := uint64(512) // fallback block size
	if err := syscall.Statfs(diskPath, &fsStat); err == nil {
		blockSize = uint64(fsStat.Bsize)
	}

	// ru.Inblock and ru.Oublock hold the number of block operations performed.
	readCount := ru.Inblock
	writeCount := ru.Oublock

	// Convert block counts to estimated bytes.
	readBytes := readCount * int64(blockSize)
	writeBytes := writeCount * int64(blockSize)

	// Populate the IOCountersStat–like struct.
	ioCounters := &process.IOCountersStat{
		ReadCount:      uint64(readCount),
		WriteCount:     uint64(writeCount),
		ReadBytes:      uint64(readBytes),
		WriteBytes:     uint64(writeBytes),
		DiskReadBytes:  uint64(readBytes),
		DiskWriteBytes: uint64(writeBytes),
	}

	return ioCounters, nil
}

// Start takes a snapshot marking the beginning of an interval.
func (m *Monitor) Start() *Snapshot {
	snap, err := m.Snapshot()
	if err != nil {
		log.Fatalf("error taking start snapshot: %v", err)
	}
	return snap
}

// Stop takes a snapshot marking the end of an interval.
func (m *Monitor) Stop() *Snapshot {
	snap, err := m.Snapshot()
	if err != nil {
		log.Fatalf("error taking stop snapshot: %v", err)
	}
	return snap
}

// SnapshotDiff represents the difference between two snapshots.
type SnapshotDiff struct {
	Duration         time.Duration `json:"duration"`            // Elapsed time.
	CPUPercent       float64       `json:"cpu_percent"`         // Approximate CPU usage percentage.
	MemoryDiff       int64         `json:"memory_diff"`         // Difference in RSS (bytes).
	IOReadCountDiff  int64         `json:"io_read_count_diff"`  // Difference in IO read count.
	IOWriteCountDiff int64         `json:"io_write_count_diff"` // Difference in IO write count.
	IOReadBytesDiff  int64         `json:"io_read_bytes_diff"`  // Difference in IO read bytes.
	IOWriteBytesDiff int64         `json:"io_write_bytes_diff"` // Difference in IO write bytes.
	ThreadsDiff      int32         `json:"threads_diff"`        // Difference in thread count.
	DiskUsedDiff     int64         `json:"disk_used_diff"`      // Difference in disk used bytes.
	SwapUsedDiff     int64         `json:"swap_used_diff"`      // Difference in swap used bytes.
	// Runtime memory differences from Go's runtime.ReadMemStats.
	HeapAllocDiff int64 `json:"heap_alloc_diff"` // Change in heap allocation.
	HeapSysDiff   int64 `json:"heap_sys_diff"`   // Change in memory obtained from system.
}

// Diff computes the difference between the current snapshot and a previous one.
func (s *Snapshot) Diff(prev *Snapshot) *SnapshotDiff {
	duration := s.Timestamp.Sub(prev.Timestamp)
	cpuDelta := s.CPUTime - prev.CPUTime
	cpuPercent := (cpuDelta / duration.Seconds()) * 100

	memDiff := int64(s.Memory) - int64(prev.Memory)
	ioReadCountDiff := int64(s.IOCounters.ReadCount) - int64(prev.IOCounters.ReadCount)
	ioWriteCountDiff := int64(s.IOCounters.WriteCount) - int64(prev.IOCounters.WriteCount)
	ioReadBytesDiff := int64(s.IOCounters.ReadBytes) - int64(prev.IOCounters.ReadBytes)
	ioWriteBytesDiff := int64(s.IOCounters.WriteBytes) - int64(prev.IOCounters.WriteBytes)
	threadsDiff := s.Threads - prev.Threads
	diskUsedDiff := int64(s.DiskUsage.Used) - int64(prev.DiskUsage.Used)
	swapUsedDiff := int64(s.SwapUsage.Used) - int64(prev.SwapUsage.Used)

	heapAllocDiff := int64(s.GoMemStats.HeapAlloc) - int64(prev.GoMemStats.HeapAlloc)
	heapSysDiff := int64(s.GoMemStats.HeapSys) - int64(prev.GoMemStats.HeapSys)

	return &SnapshotDiff{
		Duration:         duration,
		CPUPercent:       cpuPercent,
		MemoryDiff:       memDiff,
		IOReadCountDiff:  ioReadCountDiff,
		IOWriteCountDiff: ioWriteCountDiff,
		IOReadBytesDiff:  ioReadBytesDiff,
		IOWriteBytesDiff: ioWriteBytesDiff,
		ThreadsDiff:      threadsDiff,
		DiskUsedDiff:     diskUsedDiff,
		SwapUsedDiff:     swapUsedDiff,
		HeapAllocDiff:    heapAllocDiff,
		HeapSysDiff:      heapSysDiff,
	}
}

// ToString returns a human-readable summary of the snapshot difference.
func (d *SnapshotDiff) ToString() string {
	return fmt.Sprintf(
		"Duration: %v\nCPU/Memory Usage: %.2f%%/%s\nIO Read/Write: (%d/%s)/(%d/%s)\nThread Usage: %d\nDisk Usage: %s\nSwap Usage: %s\nHeapAlloc: %s\nHeapSys: %s",
		d.Duration,
		d.CPUPercent,
		FormatBytes(d.MemoryDiff),
		d.IOReadCountDiff,
		FormatBytes(d.IOReadBytesDiff),
		d.IOWriteCountDiff,
		FormatBytes(d.IOWriteBytesDiff),
		d.ThreadsDiff,
		FormatBytes(d.DiskUsedDiff),
		FormatBytes(d.SwapUsedDiff),
		FormatBytes(d.HeapAllocDiff),
		FormatBytes(d.HeapSysDiff),
	)
}

// ToJSON returns the JSON representation of the snapshot difference.
func (d *SnapshotDiff) ToJSON() string {
	b, err := json.MarshalIndent(d, "", "  ")
	if err != nil {
		log.Printf("error marshalling SnapshotDiff to JSON: %v", err)
		return "{}"
	}
	return string(b)
}

// FormatBytes converts a byte difference (possibly negative) to a human-readable string.
func FormatBytes(diff int64) string {
	sign := ""
	if diff > 0 {
		sign = "+"
	}
	absDiff := diff
	if diff < 0 {
		absDiff = -diff
	}

	var value float64
	unit := "B"
	switch {
	case absDiff >= 1<<30:
		value = float64(absDiff) / (1 << 30)
		unit = "GB"
	case absDiff >= 1<<20:
		value = float64(absDiff) / (1 << 20)
		unit = "MB"
	case absDiff >= 1<<10:
		value = float64(absDiff) / (1 << 10)
		unit = "KB"
	default:
		value = float64(absDiff)
	}
	return fmt.Sprintf("%s%.2f %s", sign, value, unit)
}

// Since takes a starting snapshot and returns the difference from the current usage.
func Since(start *Snapshot) *SnapshotDiff {
	monitor := New()
	current, err := monitor.Snapshot()
	if err != nil {
		log.Fatalf("error taking current snapshot: %v", err)
	}
	return current.Diff(start)
}

// StartGlobalMonitoring starts a background goroutine that takes a snapshot at the given interval
// and passes it to the callback. Useful for continuous APM monitoring.
func StartGlobalMonitoring(interval time.Duration, callback func(*Snapshot)) {
	go func() {
		monitor := New()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			snap, err := monitor.Snapshot()
			if err != nil {
				log.Printf("global monitoring error: %v", err)
				continue
			}
			callback(snap)
		}
	}()
}
