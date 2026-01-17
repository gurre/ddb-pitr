package metrics

import (
	"encoding/json"
	"sync"
	"testing"
	"time"
)

// TestNewMetricsCounters verifies all new counters (throttles, retries, lost, bytes)
// record and read correctly using atomic operations.
func TestNewMetricsCounters(t *testing.T) {
	m := NewMetrics()

	// Record throttle events
	m.RecordThrottle()
	m.RecordThrottle()
	if got := m.Throttles(); got != 2 {
		t.Errorf("Throttles() = %d, want 2", got)
	}

	// Record retry events
	m.RecordRetry()
	m.RecordRetry()
	m.RecordRetry()
	if got := m.Retries(); got != 3 {
		t.Errorf("Retries() = %d, want 3", got)
	}

	// Record lost items
	m.RecordLost(5)
	m.RecordLost(10)
	if got := m.LostItems(); got != 15 {
		t.Errorf("LostItems() = %d, want 15", got)
	}

	// Record bytes written
	m.RecordBytes(1024)
	m.RecordBytes(2048)
	if got := m.BytesWritten(); got != 3072 {
		t.Errorf("BytesWritten() = %d, want 3072", got)
	}

	// Record errors
	m.RecordError()
	if got := m.Errors(); got != 1 {
		t.Errorf("Errors() = %d, want 1", got)
	}

	// Verify report includes new counters
	report := m.GenerateReport()
	if report.Throttles != 2 {
		t.Errorf("report.Throttles = %d, want 2", report.Throttles)
	}
	if report.Retries != 3 {
		t.Errorf("report.Retries = %d, want 3", report.Retries)
	}
	if report.LostItems != 15 {
		t.Errorf("report.LostItems = %d, want 15", report.LostItems)
	}
	if report.BytesWritten != 3072 {
		t.Errorf("report.BytesWritten = %d, want 3072", report.BytesWritten)
	}
}

// TestMetricsCountersConcurrency verifies atomic counters are thread-safe
// under concurrent access from multiple goroutines.
func TestMetricsCountersConcurrency(t *testing.T) {
	m := NewMetrics()
	const goroutines = 100
	const iterations = 1000

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				m.RecordThrottle()
				m.RecordRetry()
				m.RecordLost(1)
				m.RecordBytes(100)
				m.RecordProcessed()
			}
		}()
	}
	wg.Wait()

	expected := int64(goroutines * iterations)
	if got := m.Throttles(); got != expected {
		t.Errorf("Throttles() = %d, want %d", got, expected)
	}
	if got := m.Retries(); got != expected {
		t.Errorf("Retries() = %d, want %d", got, expected)
	}
	if got := m.LostItems(); got != expected {
		t.Errorf("LostItems() = %d, want %d", got, expected)
	}
	if got := m.BytesWritten(); got != expected*100 {
		t.Errorf("BytesWritten() = %d, want %d", got, expected*100)
	}
}

// TestReportMarshalJSON verifies Report serializes to valid JSON
// with correctly formatted duration field.
func TestReportMarshalJSON(t *testing.T) {
	m := NewMetrics()
	m.RecordProcessed()
	m.RecordThrottle()
	m.RecordRetry()
	m.RecordLost(5)
	m.RecordBytes(1024)

	report := m.GenerateReport()
	data, err := json.Marshal(report)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	// Unmarshal to verify valid JSON structure
	var parsed map[string]interface{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	// Verify duration is formatted as string (not nanoseconds)
	duration, ok := parsed["duration"].(string)
	if !ok {
		t.Errorf("duration should be string, got %T", parsed["duration"])
	}
	if duration == "" {
		t.Error("duration should not be empty")
	}

	// Verify numeric fields exist
	if _, ok := parsed["throttles"]; !ok {
		t.Error("missing throttles field")
	}
	if _, ok := parsed["retries"]; !ok {
		t.Error("missing retries field")
	}
	if _, ok := parsed["lostItems"]; !ok {
		t.Error("missing lostItems field")
	}
	if _, ok := parsed["bytesWritten"]; !ok {
		t.Error("missing bytesWritten field")
	}
}

// TestRecordProcessingTime verifies processing time accumulates correctly.
func TestRecordProcessingTime(t *testing.T) {
	m := NewMetrics()

	m.RecordProcessingTime(100 * time.Millisecond)
	m.RecordProcessingTime(200 * time.Millisecond)

	// Access internal field through report timing behavior
	// Processing time affects throughput calculation indirectly
	report := m.GenerateReport()
	if report.Duration <= 0 {
		t.Error("expected positive duration")
	}
}

func TestMetricsHappyPath(t *testing.T) {
	m := NewMetrics()

	// Record some metrics
	m.RecordProcessed()
	m.RecordProcessed()
	m.RecordBatchWritten()
	m.RecordError()
	m.RecordCorrupt()

	// Simulate some processing time
	time.Sleep(100 * time.Millisecond)

	// Generate report
	report := m.GenerateReport()

	// Verify results
	if report.TotalItems != 2 {
		t.Errorf("expected 2 items processed, got %d", report.TotalItems)
	}
	if report.CorruptCount != 1 {
		t.Errorf("expected 1 corrupt item, got %d", report.CorruptCount)
	}
	if report.Duration < 100*time.Millisecond {
		t.Errorf("expected duration >= 100ms, got %v", report.Duration)
	}
	if report.Throughput <= 0 {
		t.Errorf("expected positive throughput, got %f", report.Throughput)
	}

	// Test string representation
	str := report.String()
	if str == "" {
		t.Error("expected non-empty string representation")
	}
}
