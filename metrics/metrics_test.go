package metrics

import (
	"encoding/json"
	"strings"
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
	if got := m.BytesRead(); got != 3072 {
		t.Errorf("BytesRead() = %d, want 3072", got)
	}

	// Record errors
	m.RecordError()
	if got := m.Errors(); got != 1 {
		t.Errorf("Errors() = %d, want 1", got)
	}

	// Verify report includes new counters
	report := m.GenerateReport(0)
	if report.Throttles != 2 {
		t.Errorf("report.Throttles = %d, want 2", report.Throttles)
	}
	if report.Retries != 3 {
		t.Errorf("report.Retries = %d, want 3", report.Retries)
	}
	if report.LostItems != 15 {
		t.Errorf("report.LostItems = %d, want 15", report.LostItems)
	}
	if report.BytesRead != 3072 {
		t.Errorf("report.BytesRead = %d, want 3072", report.BytesRead)
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
				m.RecordProcessed(1)
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
	if got := m.BytesRead(); got != expected*100 {
		t.Errorf("BytesRead() = %d, want %d", got, expected*100)
	}
}

// TestReportMarshalJSON verifies Report serializes to valid JSON
// with correctly formatted duration field.
func TestReportMarshalJSON(t *testing.T) {
	m := NewMetrics()
	m.RecordProcessed(1)
	m.RecordThrottle()
	m.RecordRetry()
	m.RecordLost(5)
	m.RecordBytes(1024)

	report := m.GenerateReport(0)
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

	// Both durations are rendered the same way, so a reader never has to guess whether a
	// field is nanoseconds or a formatted span.
	if _, ok := parsed["processingTime"].(string); !ok {
		t.Errorf("processingTime should be string, got %T", parsed["processingTime"])
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
	if _, ok := parsed["bytesRead"]; !ok {
		t.Error("missing bytesRead field")
	}
}

// TestProcessingTimeIsSafeUnderConcurrentWriters verifies the accumulated processing
// time is guarded. Every worker records into it after each batch while the report reads
// it, so unsynchronised access is a live data race that -race exposes here.
func TestProcessingTimeIsSafeUnderConcurrentWriters(t *testing.T) {
	m := NewMetrics()
	const workers = 8
	const batches = 50

	var wg sync.WaitGroup
	wg.Add(workers + 1)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < batches; j++ {
				m.RecordProcessingTime(time.Millisecond)
			}
		}()
	}
	go func() {
		defer wg.Done()
		for j := 0; j < batches; j++ {
			m.GenerateReport(0)
		}
	}()
	wg.Wait()

	if got := m.GenerateReport(0).ProcessingTime; got != workers*batches*time.Millisecond {
		t.Errorf("ProcessingTime = %v, want %v", got, workers*batches*time.Millisecond)
	}
}

// TestReportCarriesTheWholeRun verifies every counter reaches the report. The report is
// the only durable record once the process exits, so a field left at its zero value
// would tell an operator a restore hit no throttling and lost nothing when it did.
func TestReportCarriesTheWholeRun(t *testing.T) {
	m := NewMetrics()
	// Every value is distinct so a field taking another's value cannot look correct.
	for i := 0; i < 7; i++ {
		m.RecordProcessed(1)
	}
	for i := 0; i < 3; i++ {
		m.RecordBatchWritten()
	}
	for i := 0; i < 2; i++ {
		m.RecordThrottle()
	}
	for i := 0; i < 4; i++ {
		m.RecordRetry()
	}
	m.RecordLost(6)
	m.RecordBytes(8)
	m.RecordProcessingTime(9 * time.Millisecond)

	report := m.GenerateReport(5)

	if report.StartTime.IsZero() {
		t.Error("report carries no start time")
	}
	if !report.EndTime.After(report.StartTime) {
		t.Errorf("report ends at %v, which is not after its start %v", report.EndTime, report.StartTime)
	}
	if report.Duration <= 0 {
		t.Errorf("Duration = %v, want a positive span", report.Duration)
	}
	for _, tt := range []struct {
		name string
		got  int64
		want int64
	}{
		{"TotalItems", report.TotalItems, 7},
		{"BatchesWritten", report.BatchesWritten, 3},
		{"CorruptCount", report.CorruptCount, 5},
		{"Throttles", report.Throttles, 2},
		{"Retries", report.Retries, 4},
		{"LostItems", report.LostItems, 6},
		{"BytesRead", report.BytesRead, 8},
	} {
		if tt.got != tt.want {
			t.Errorf("report.%s = %d, want %d", tt.name, tt.got, tt.want)
		}
	}
	if report.ProcessingTime != 9*time.Millisecond {
		t.Errorf("report.ProcessingTime = %v, want 9ms", report.ProcessingTime)
	}
}

// TestReportStringLabelsEveryCount verifies the console summary puts each number against
// its own label. Throttles, retries and lost items call for different responses from an
// operator, so reading one as another sends them the wrong way.
func TestReportStringLabelsEveryCount(t *testing.T) {
	// Distinct values, so a transposed pair cannot look correct.
	report := Report{
		Duration:       90 * time.Second,
		TotalItems:     11,
		BatchesWritten: 22,
		CorruptCount:   33,
		Throughput:     44,
		Throttles:      55,
		Retries:        66,
		LostItems:      77,
	}

	const want = "Restore completed in 1m30s\n" +
		"Total items: 11 in 22 batches\n" +
		"Corrupt items: 33\n" +
		"Throughput: 44.00 items/sec (0.00 MB/s)\n" +
		"Data read: 0.00 MB\n" +
		"Throttles: 55 | Retries: 66 | Lost: 77"
	if got := report.String(); got != want {
		t.Errorf("report string =\n%s\nwant\n%s", got, want)
	}
}

// TestRecordProcessingTime verifies time spent writing accumulates into the report,
// where it is what tells an operator whether the restore was limited by DynamoDB or
// by reading the export.
func TestRecordProcessingTime(t *testing.T) {
	m := NewMetrics()

	m.RecordProcessingTime(100 * time.Millisecond)
	m.RecordProcessingTime(200 * time.Millisecond)

	if got := m.GenerateReport(0).ProcessingTime; got != 300*time.Millisecond {
		t.Errorf("ProcessingTime = %v, want 300ms", got)
	}
}

// TestReportCountsBatches verifies batch writes reach the report, which is the durable
// record of how many DynamoDB calls the restore cost.
func TestReportCountsBatches(t *testing.T) {
	m := NewMetrics()

	m.RecordBatchWritten()
	m.RecordBatchWritten()
	m.RecordBatchWritten()

	if got := m.GenerateReport(0).BatchesWritten; got != 3 {
		t.Errorf("BatchesWritten = %d, want 3", got)
	}
}

// TestReportDividesWorkByElapsedTime verifies throughput and byte rate are the recorded
// totals divided by the elapsed time. Both feed the summary an operator uses to size the
// next restore, so a rate that multiplies instead of divides would badly mislead.
func TestReportDividesWorkByElapsedTime(t *testing.T) {
	m := NewMetrics()
	// Backdate the start so the elapsed time is a known interval rather than microseconds.
	m.startTime = time.Now().Add(-2 * time.Second)

	for i := 0; i < 100; i++ {
		m.RecordProcessed(1)
	}
	m.RecordBytes(2048)

	report := m.GenerateReport(0)

	// Elapsed is just over two seconds, so the rates sit just under half the totals.
	if report.Throughput > 50 || report.Throughput < 49 {
		t.Errorf("Throughput = %f, want about 50 items/sec", report.Throughput)
	}
	if report.ByteRate > 1024 || report.ByteRate < 1000 {
		t.Errorf("ByteRate = %f, want about 1024 bytes/sec", report.ByteRate)
	}
}

// TestReportStringConvertsBytesToMegabytes verifies the console summary reports megabytes,
// since the byte counters are meaningless to read at restore scale.
func TestReportStringConvertsBytesToMegabytes(t *testing.T) {
	report := Report{
		BytesRead:  1000 * 1024 * 1024,
		ByteRate:   500 * 1024 * 1024,
		TotalItems: 7,
	}

	str := report.String()

	// The full field is matched, since a substring would also be found inside a much
	// larger wrong number.
	if !strings.Contains(str, "Data read: 1000.00 MB\n") {
		t.Errorf("expected 1000.00 MB written in:\n%s", str)
	}
	if !strings.Contains(str, "(500.00 MB/s)") {
		t.Errorf("expected 500.00 MB/s in:\n%s", str)
	}
}

func TestMetricsHappyPath(t *testing.T) {
	m := NewMetrics()

	// Record some metrics
	m.RecordProcessed(1)
	m.RecordProcessed(1)
	m.RecordBatchWritten()
	m.RecordError()

	// Simulate some processing time
	time.Sleep(100 * time.Millisecond)

	// Generate report
	report := m.GenerateReport(1)

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
