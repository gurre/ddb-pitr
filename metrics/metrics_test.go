package metrics

import (
	"testing"
	"time"
)

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
