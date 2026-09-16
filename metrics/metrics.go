// Package metrics counts what a restore did and renders the final report.
package metrics

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	json "github.com/goccy/go-json"
)

// Metrics is the set of counters every worker reports into; each is updated
// atomically so workers never wait on one another.
// Fields ordered largest to smallest for memory alignment.
type Metrics struct {
	mu sync.RWMutex

	// Histograms for performance analysis
	processingTime time.Duration // Total time spent processing records
	startTime      time.Time     // When the restore operation started

	// Counters (all use atomic operations)
	recordsProcessed int64 // Items written to the table
	batchesWritten   int64 // Number of batches written to DynamoDB
	errors           int64 // Number of errors encountered
	throttles        int64 // Number of throttle events (ProvisionedThroughputExceeded)
	retries          int64 // Number of successful retries after transient failures
	lostItems        int64 // Number of items that failed permanently
	bytesRead        int64 // Export bytes read behind the items written
}

// NewMetrics creates a new Metrics instance with initialized counters
func NewMetrics() *Metrics {
	return &Metrics{
		startTime: time.Now(),
	}
}

// RecordProcessed adds n to the count of items written to the table
func (m *Metrics) RecordProcessed(n int64) {
	atomic.AddInt64(&m.recordsProcessed, n)
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

// RecordLost adds to the lost items counter
func (m *Metrics) RecordLost(n int64) {
	atomic.AddInt64(&m.lostItems, n)
}

// RecordBytes adds to the export bytes read behind items written
func (m *Metrics) RecordBytes(n int64) {
	atomic.AddInt64(&m.bytesRead, n)
}

// Throttles returns the current throttle count
func (m *Metrics) Throttles() int64 {
	return atomic.LoadInt64(&m.throttles)
}

// Retries returns the current retry count
func (m *Metrics) Retries() int64 {
	return atomic.LoadInt64(&m.retries)
}

// LostItems returns the current lost items count
func (m *Metrics) LostItems() int64 {
	return atomic.LoadInt64(&m.lostItems)
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

// Report is the outcome of a restore, printed at the end and uploaded when asked for.
type Report struct {
	StartTime time.Time     `json:"startTime"` // When the restore operation started
	EndTime   time.Time     `json:"endTime"`   // When the restore operation completed
	Duration  time.Duration `json:"duration"`  // Wall clock time the operation took
	// ProcessingTime is the time workers spent writing, summed across the pool. Held
	// against Duration it separates a restore limited by DynamoDB from one limited by S3.
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

	totalItems := atomic.LoadInt64(&m.recordsProcessed)
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
		LostItems:      atomic.LoadInt64(&m.lostItems),
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
	mbRead := float64(r.BytesRead) / (1024 * 1024)
	mbPerSec := r.ByteRate / (1024 * 1024)

	return fmt.Sprintf(
		"Restore completed in %s\n"+
			"Total items: %d in %d batches\n"+
			"Corrupt items: %d\n"+
			"Throughput: %.2f items/sec (%.2f MB/s)\n"+
			"Data read: %.2f MB\n"+
			"Throttles: %d | Retries: %d | Lost: %d",
		r.Duration,
		r.TotalItems,
		r.BatchesWritten,
		r.CorruptCount,
		r.Throughput,
		mbPerSec,
		mbRead,
		r.Throttles,
		r.Retries,
		r.LostItems,
	)
}
