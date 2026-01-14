// Package metrics implements the metrics collection and reporting functionality as specified
// in section 6 of the design specification. It handles collecting counters and histograms
// during the restore operation and generating the final report.
package metrics

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	json "github.com/goccy/go-json"
)

// Metrics collects counters and histograms as defined in section 6 of the spec.
// It uses atomic operations for thread-safe counter updates.
type Metrics struct {
	mu sync.RWMutex

	// Counters as specified in section 6
	recordsProcessed int64 // Total number of records processed
	batchesWritten   int64 // Number of batches written to DynamoDB
	errors           int64 // Number of errors encountered
	corruptCount     int64 // Number of corrupt records found

	// Histograms for performance analysis
	processingTime time.Duration // Total time spent processing records
	startTime      time.Time     // When the restore operation started
}

// NewMetrics creates a new Metrics instance with initialized counters
func NewMetrics() *Metrics {
	return &Metrics{
		startTime: time.Now(),
	}
}

// RecordProcessed increments the processed records counter
func (m *Metrics) RecordProcessed() {
	atomic.AddInt64(&m.recordsProcessed, 1)
}

// RecordBatchWritten increments the written batches counter
func (m *Metrics) RecordBatchWritten() {
	atomic.AddInt64(&m.batchesWritten, 1)
}

// RecordError increments the errors counter
func (m *Metrics) RecordError() {
	atomic.AddInt64(&m.errors, 1)
}

// RecordCorrupt increments the corrupt records counter
func (m *Metrics) RecordCorrupt() {
	atomic.AddInt64(&m.corruptCount, 1)
}

// RecordProcessingTime records the processing time for a batch
func (m *Metrics) RecordProcessingTime(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.processingTime += d
}

// Report contains the final metrics report as defined in section 6 of the spec.
// It includes all required fields for the JSON report output.
type Report struct {
	StartTime    time.Time     `json:"startTime"`    // When the restore operation started
	EndTime      time.Time     `json:"endTime"`      // When the restore operation completed
	TotalItems   int64         `json:"totalItems"`   // Total number of items processed
	CorruptCount int64         `json:"corruptCount"` // Number of corrupt items found
	Duration     time.Duration `json:"duration"`     // Total duration of the operation
	Throughput   float64       `json:"throughput"`   // Items processed per second
}

// GenerateReport generates a final report as specified in section 6.
// It calculates all metrics and returns a Report struct ready for JSON output.
func (m *Metrics) GenerateReport() Report {
	endTime := time.Now()
	duration := endTime.Sub(m.startTime)

	// Calculate throughput (items per second)
	var throughput float64
	if duration > 0 {
		throughput = float64(atomic.LoadInt64(&m.recordsProcessed)) / duration.Seconds()
	}

	return Report{
		StartTime:    m.startTime,
		EndTime:      endTime,
		TotalItems:   atomic.LoadInt64(&m.recordsProcessed),
		CorruptCount: atomic.LoadInt64(&m.corruptCount),
		Duration:     duration,
		Throughput:   throughput,
	}
}

// MarshalJSON implements json.Marshaler to format the report as JSON
// as required by section 6 for stdout and S3 output.
func (r Report) MarshalJSON() ([]byte, error) {
	type Alias Report
	return json.Marshal(&struct {
		Alias
		Duration string `json:"duration"`
	}{
		Alias:    Alias(r),
		Duration: r.Duration.String(),
	})
}

// String returns a human-readable string representation of the report
// as specified in section 6 for console output.
func (r Report) String() string {
	return fmt.Sprintf(
		"Restore completed in %s\n"+
			"Total items: %d\n"+
			"Corrupt items: %d\n"+
			"Throughput: %.2f items/sec",
		r.Duration,
		r.TotalItems,
		r.CorruptCount,
		r.Throughput,
	)
}
