// Package metrics counts what a restore did and renders the final report.
package metrics

import (
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	json "github.com/goccy/go-json"
	"github.com/gurre/ddb-pitr/itemimage"
)

// Metrics is the set of counters every reader and batch reports into; each is updated
// atomically so none waits on another.
// Fields ordered largest to smallest for memory alignment.
type Metrics struct {
	mu sync.RWMutex

	// Histograms for performance analysis
	processingTime time.Duration // Total time spent processing records
	startTime      time.Time     // When the restore operation started

	// Per kind of operation: how many the table took, how many it refused or handed
	// back to be sent again, and how many were given up. Indexed by OperationType.
	applied  [itemimage.OperationKinds]int64
	rejected [itemimage.OperationKinds]int64
	lost     [itemimage.OperationKinds]int64

	// Counters (all use atomic operations)
	batchesWritten int64 // Number of batches written to DynamoDB
	errors         int64 // Number of errors encountered
	throttles      int64 // Number of throttle events (ProvisionedThroughputExceeded)
	retries        int64 // Number of successful retries after transient failures
	bytesRead      int64 // Export bytes read behind the items written
	// pace is the write rate the table is currently allowing, in write capacity units
	// per second, held as the bits of a float64. Zero means nothing is pacing the
	// writes yet, which is where every restore starts.
	pace uint64
	// concurrency is how many writes may be in flight at once, as the writer last set it.
	concurrency int64
}

// NewMetrics creates a new Metrics instance with initialized counters
func NewMetrics() *Metrics {
	return &Metrics{
		startTime: time.Now(),
	}
}

// RecordApplied adds items of one kind the table took, and the export bytes behind them.
func (m *Metrics) RecordApplied(kind itemimage.OperationType, items, bytes int64) {
	atomic.AddInt64(&m.applied[kind], items)
	atomic.AddInt64(&m.bytesRead, bytes)
}

// RecordRejected adds items of one kind the table refused or handed back, which the
// restore sends again. They are not lost; the count says how hard the table is pushing
// back, and on which kind of change.
func (m *Metrics) RecordRejected(kind itemimage.OperationType, n int64) {
	atomic.AddInt64(&m.rejected[kind], n)
}

// RecordBatchWritten increments the written batches counter
func (m *Metrics) RecordBatchWritten() {
	atomic.AddInt64(&m.batchesWritten, 1)
}

// RecordError increments the errors counter
func (m *Metrics) RecordError() {
	atomic.AddInt64(&m.errors, 1)
}

// RecordThrottle increments the throttle events counter
func (m *Metrics) RecordThrottle() {
	atomic.AddInt64(&m.throttles, 1)
}

// RecordRetry increments the successful retries counter
func (m *Metrics) RecordRetry() {
	atomic.AddInt64(&m.retries, 1)
}

// RecordLost adds items of one kind given up with a batch that failed.
func (m *Metrics) RecordLost(kind itemimage.OperationType, n int64) {
	atomic.AddInt64(&m.lost[kind], n)
}

// RecordConcurrency records how many writes may be in flight at once. It is a gauge: the
// last value is the one that describes the restore now.
func (m *Metrics) RecordConcurrency(limit int) {
	atomic.StoreInt64(&m.concurrency, int64(limit))
}

// Concurrency returns how many writes may be in flight at once, or zero before the
// writer has said.
func (m *Metrics) Concurrency() int {
	return int(atomic.LoadInt64(&m.concurrency))
}

// Processed returns the items of every kind the table has taken.
func (m *Metrics) Processed() int64 {
	return sum(&m.applied)
}

// BatchesWritten returns the batches written so far.
func (m *Metrics) BatchesWritten() int64 {
	return atomic.LoadInt64(&m.batchesWritten)
}

// Operations returns, per kind of operation, what has happened to them so far.
func (m *Metrics) Operations() Operations {
	var ops Operations
	for kind := range ops {
		ops[kind] = KindCount{
			Applied:  atomic.LoadInt64(&m.applied[kind]),
			Rejected: atomic.LoadInt64(&m.rejected[kind]),
			Lost:     atomic.LoadInt64(&m.lost[kind]),
		}
	}
	return ops
}

// sum totals a per-kind counter.
func sum(counts *[itemimage.OperationKinds]int64) int64 {
	var total int64
	for kind := range counts {
		total += atomic.LoadInt64(&counts[kind])
	}
	return total
}

// RecordPace records the write rate the table is currently allowing, in write capacity
// units per second. It is a gauge rather than a counter: the last value written is the
// one that describes the restore now, and an operator watching a slow restore reads it
// as the table's limit rather than a mystery.
func (m *Metrics) RecordPace(wcuPerSecond float64) {
	atomic.StoreUint64(&m.pace, math.Float64bits(wcuPerSecond))
}

// Pace returns the write rate the table is currently allowing, and whether anything is
// pacing the writes at all. A restore that has never been throttled is not paced, and
// reporting zero for that would read as a stalled restore.
func (m *Metrics) Pace() (float64, bool) {
	bits := atomic.LoadUint64(&m.pace)
	if bits == 0 {
		return 0, false
	}
	return math.Float64frombits(bits), true
}

// Throttles returns the current throttle count
func (m *Metrics) Throttles() int64 {
	return atomic.LoadInt64(&m.throttles)
}

// Retries returns the current retry count
func (m *Metrics) Retries() int64 {
	return atomic.LoadInt64(&m.retries)
}

// LostItems returns the items of every kind given up so far.
func (m *Metrics) LostItems() int64 {
	return sum(&m.lost)
}

// BytesRead returns the export bytes read behind every item written so far
func (m *Metrics) BytesRead() int64 {
	return atomic.LoadInt64(&m.bytesRead)
}

// Errors returns the current error count
func (m *Metrics) Errors() int64 {
	return atomic.LoadInt64(&m.errors)
}

// RecordProcessingTime records the processing time for a batch
func (m *Metrics) RecordProcessingTime(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.processingTime += d
}

// KindCount is what happened to the operations of one kind.
// Fields are ordered largest-to-smallest for memory alignment.
type KindCount struct {
	Applied  int64 `json:"applied"`  // Taken by the table
	Rejected int64 `json:"rejected"` // Refused or handed back, and sent again
	Lost     int64 `json:"lost"`     // Given up with a batch that failed
}

// Operations is what happened to each kind of operation, indexed by OperationType. It
// is how a restore is checked against its export: an incremental export says how many
// items it inserted, changed and deleted, and a restore that applied none of its deletes
// looks, by its total alone, like one that is nearly done.
type Operations [itemimage.OperationKinds]KindCount

// MarshalJSON renders the kinds by name, every kind present, so a reader need not know
// the numbering and a kind the export did not hold reads as zero rather than missing.
func (o Operations) MarshalJSON() ([]byte, error) {
	byName := make(map[string]KindCount, len(o))
	for kind, count := range o {
		byName[itemimage.OperationType(kind).String()] = count
	}
	return json.Marshal(byName)
}

// reportOrder is the order kinds are named in: inserts, then changes, then deletes, the
// order an operator reads an incremental export in.
var reportOrder = [itemimage.OperationKinds]itemimage.OperationType{itemimage.OpPut, itemimage.OpUpdate, itemimage.OpDelete}

// String renders the kinds that occurred as "put 12 update 3 delete 1", or "none" when
// nothing has been applied.
func (o Operations) String() string {
	var b strings.Builder
	for _, kind := range reportOrder {
		count := o[kind]
		if count.Applied == 0 {
			continue
		}
		if b.Len() > 0 {
			b.WriteByte(' ')
		}
		fmt.Fprintf(&b, "%s %d", kind, count.Applied)
	}
	if b.Len() == 0 {
		return "none"
	}
	return b.String()
}

// Report is the outcome of a restore, printed at the end and uploaded when asked for.
type Report struct {
	StartTime time.Time     `json:"startTime"` // When the restore operation started
	EndTime   time.Time     `json:"endTime"`   // When the restore operation completed
	Duration  time.Duration `json:"duration"`  // Wall clock time the operation took
	// ProcessingTime is the time batches spent with the writer, from when each was handed
	// over, a wait for a free slot included, to when it was answered, summed over batches
	// in flight at the same time. Held against Duration it says how much of the restore
	// was spent waiting on the table.
	ProcessingTime time.Duration `json:"processingTime"`
	TotalItems     int64         `json:"totalItems"`     // Items written to the table; skipped lines are not counted
	BatchesWritten int64         `json:"batchesWritten"` // Number of batches written to DynamoDB
	// CorruptCount is how many export lines could not be decoded. Unlike every other
	// count here it belongs to the restore rather than to this run of it: those lines
	// are lost for good, and a resumed run does not re-read the files they are in.
	CorruptCount int64   `json:"corruptCount"`
	Throttles    int64   `json:"throttles"`  // Number of throttle events
	Retries      int64   `json:"retries"`    // Number of successful retries
	LostItems    int64   `json:"lostItems"`  // Number of items that failed permanently
	BytesRead    int64   `json:"bytesRead"`  // Export bytes read behind the items written
	Throughput   float64 `json:"throughput"` // Items processed per second
	ByteRate     float64 `json:"byteRate"`   // Bytes per second
	// Operations is what happened to each kind of operation: put, update and delete.
	Operations Operations `json:"operations"`
}

// GenerateReport renders the counters into a Report as of now. The skipped-line count
// comes from the caller because it is the one number here that spans runs: the counters
// hold what this process did, while a resumed restore has to report every line the
// export lost, including those an earlier run read past.
// Example:
//
//	report := m.GenerateReport(0) // a restore that skipped nothing
//	fmt.Println(report)
func (m *Metrics) GenerateReport(skipped int64) Report {
	endTime := time.Now()
	duration := endTime.Sub(m.startTime)

	totalItems := m.Processed()
	bytesRead := atomic.LoadInt64(&m.bytesRead)

	m.mu.RLock()
	processingTime := m.processingTime
	m.mu.RUnlock()

	// Calculate throughput (items per second) and byte rate
	var throughput, byteRate float64
	if duration > 0 {
		throughput = float64(totalItems) / duration.Seconds()
		byteRate = float64(bytesRead) / duration.Seconds()
	}

	return Report{
		StartTime:      m.startTime,
		EndTime:        endTime,
		Duration:       duration,
		ProcessingTime: processingTime,
		TotalItems:     totalItems,
		BatchesWritten: atomic.LoadInt64(&m.batchesWritten),
		CorruptCount:   skipped,
		Throttles:      atomic.LoadInt64(&m.throttles),
		Retries:        atomic.LoadInt64(&m.retries),
		LostItems:      m.LostItems(),
		Operations:     m.Operations(),
		BytesRead:      bytesRead,
		Throughput:     throughput,
		ByteRate:       byteRate,
	}
}

// MarshalJSON renders the durations as strings, which is how an operator reads them.
func (r Report) MarshalJSON() ([]byte, error) {
	type Alias Report
	return json.Marshal(&struct {
		Alias
		Duration       string `json:"duration"`
		ProcessingTime string `json:"processingTime"`
	}{
		Alias:          Alias(r),
		Duration:       r.Duration.String(),
		ProcessingTime: r.ProcessingTime.String(),
	})
}

// String renders the report for the console.
func (r Report) String() string {
	// A report with nothing applied has no breakdown worth a pair of brackets.
	breakdown := ""
	if kinds := r.Operations.String(); kinds != "none" {
		breakdown = " (" + kinds + ")"
	}
	mbRead := float64(r.BytesRead) / (1024 * 1024)
	mbPerSec := r.ByteRate / (1024 * 1024)

	return fmt.Sprintf(
		"Restore completed in %s\n"+
			"Total items: %d in %d batches%s\n"+
			"Corrupt items: %d\n"+
			"Throughput: %.2f items/sec (%.2f MB/s)\n"+
			"Data read: %.2f MB\n"+
			"Throttles: %d | Retries: %d | Lost: %d",
		r.Duration,
		r.TotalItems,
		r.BatchesWritten,
		breakdown,
		r.CorruptCount,
		r.Throughput,
		mbPerSec,
		mbRead,
		r.Throttles,
		r.Retries,
		r.LostItems,
	)
}
