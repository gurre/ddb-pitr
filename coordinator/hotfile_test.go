package coordinator

import (
	"context"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/gurre/ddb-pitr/writer"
)

// TestHandBackSlowsOnlyTheFileItCameFrom verifies an item handed back halves the window
// of the file it was read from and leaves every other file's alone. A file is one slice
// of the key space, so the shortage is that slice's; slowing the other files would slow
// partitions the table has capacity for.
func TestHandBackSlowsOnlyTheFileItCameFrom(t *testing.T) {
	coord, _ := newTestCoordinator(t, testDeps{})
	hot, cold := newFileLedger("hot", noOffset), newFileLedger("cold", noOffset)
	hot.dispatch(1)
	cold.dispatch(1)

	coord.settle([]item{{ledger: hot, offset: 1}, {ledger: cold, offset: 1}}, writer.Rejection{HandedBack: []int{0}})

	if hot.window != fileWindow/2 || cold.window < fileWindow {
		t.Errorf("windows after a hand-back from hot: hot %v, cold %v; want %v and at least %v",
			hot.window, cold.window, fileWindow/2, fileWindow)
	}
	if coord.lane.len() != 1 {
		t.Errorf("expected the handed-back item waiting to be sent again, lane holds %d", coord.lane.len())
	}
}

// TestRefusalSpreadAcrossFilesSlowsNoFile verifies items refused from several files are
// sent again without slowing any of them. A refusal that spans files is the table as a
// whole running short, which the writer's pacing answers; cutting every file's window
// as well would answer it twice and collapse the restore.
func TestRefusalSpreadAcrossFilesSlowsNoFile(t *testing.T) {
	coord, _ := newTestCoordinator(t, testDeps{})
	a, b := newFileLedger("a", noOffset), newFileLedger("b", noOffset)
	a.dispatch(1)
	b.dispatch(1)

	coord.settle([]item{{ledger: a, offset: 1}, {ledger: b, offset: 1}}, writer.Rejection{Refused: []int{0, 1}})

	if a.window != fileWindow || b.window != fileWindow {
		t.Errorf("windows after a refusal across files: %v and %v, want both %v", a.window, b.window, fileWindow)
	}
	if coord.lane.len() != 2 {
		t.Errorf("expected both refused items waiting to be sent again, lane holds %d", coord.lane.len())
	}
}

// TestRefusalFromOneFileSlowsThatFile verifies items refused all from one file slow that
// file, the way items handed back do. Once the table's rate holds calls small, a hot
// partition's items come back as whole calls refused rather than handed back from larger
// ones, and leaving them to the pacer would slow the whole table to that partition.
func TestRefusalFromOneFileSlowsThatFile(t *testing.T) {
	coord, _ := newTestCoordinator(t, testDeps{})
	hot := newFileLedger("hot", noOffset)
	hot.dispatch(1)
	hot.dispatch(2)

	coord.settle([]item{{ledger: hot, offset: 1}, {ledger: hot, offset: 2}}, writer.Rejection{Refused: []int{0, 1}})

	if hot.window != fileWindow/2 {
		t.Errorf("window = %v after a refusal from one file, want %v", hot.window, fileWindow/2)
	}
}

// TestItemsTurnedAwayWaitBeforeGoingAgain verifies an item the table refused is not sent
// straight back. Resent at once, a refused item meets the same shortage and spins through
// the writer as fast as it can be refused.
func TestItemsTurnedAwayWaitBeforeGoingAgain(t *testing.T) {
	coord, _ := newTestCoordinator(t, testDeps{})
	l := newFileLedger("f", noOffset)
	for offset := int64(1); offset <= 25; offset++ {
		l.dispatch(offset)
	}
	batch := make([]item, 25)
	refused := make([]int, 25)
	for i := range batch {
		batch[i] = item{ledger: l, offset: int64(i + 1)}
		refused[i] = i
	}

	settled := time.Now()
	coord.settle(batch, writer.Rejection{Refused: refused})
	if now := coord.lane.takeDue(settled, nil, 25); len(now) == 25 {
		t.Error("all 25 refused items were due to go again at once")
	}
}

// TestHandBacksAlreadyInFlightCutTheWindowOnce verifies several of a file's items handed
// back from lines sent before the last cut halve its window once. They were all in flight
// when the table first said the file's partitions were short; counting each as a new
// signal would collapse the file to one line over one shortage.
func TestHandBacksAlreadyInFlightCutTheWindowOnce(t *testing.T) {
	l := newFileLedger("f", noOffset)
	for offset := int64(1); offset <= 10; offset++ {
		l.dispatch(offset)
	}
	for offset := int64(1); offset <= 5; offset++ {
		l.reject(offset)
	}
	if l.window != fileWindow/2 {
		t.Fatalf("window = %v after five hand-backs from one flight, want %v", l.window, fileWindow/2)
	}

	l.dispatch(11) // Sent after the cut, so its hand-back is news.
	l.reject(11)
	if l.window != fileWindow/4.0 {
		t.Errorf("window = %v after a hand-back sent since the cut, want %v", l.window, fileWindow/4.0)
	}
}

// TestAFileAtItsWindowWaitsForALineToBeTaken verifies a reader whose file has as many
// lines in flight as its window allows waits, and goes on as soon as one is acknowledged.
func TestAFileAtItsWindowWaitsForALineToBeTaken(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := newFileLedger("f", noOffset)
		l.window = 1
		l.dispatch(1)

		room := make(chan error, 1)
		go func() { room <- l.waitRoom(context.Background()) }()
		synctest.Wait()
		select {
		case <-room:
			t.Fatal("a file at its window was let send another line")
		default:
		}

		l.ack(1)
		if err := <-room; err != nil {
			t.Fatalf("waitRoom = %v", err)
		}
	})
}

// TestAWindowGrowsBackWhileItIsTheLimit verifies a file's window grows as its lines are
// taken while it is holding the file back, so a partition that recovers gets its file
// back to full speed without anyone restarting the restore.
func TestAWindowGrowsBackWhileItIsTheLimit(t *testing.T) {
	l := newFileLedger("f", noOffset)
	l.window = 2
	for offset := int64(1); offset <= 40; offset++ {
		if float64(l.inFlight) >= l.window {
			l.ack(offset - 2)
		}
		l.dispatch(offset)
	}
	if l.window <= 2 {
		t.Errorf("window stayed at %v while every line was taken", l.window)
	}
}

// FuzzWatermarkNeverPassesALineInFlight checks, for any order in which a file's lines are
// dispatched, handed back, acknowledged and skipped, that the offset a checkpoint would
// record never passes a line that is still unwritten. A watermark that did would let a
// resume skip a line nothing wrote.
func FuzzWatermarkNeverPassesALineInFlight(f *testing.F) {
	f.Add([]byte{0, 0, 0, 1, 2, 1, 3})
	f.Add([]byte{0, 3, 0, 0, 2, 1, 1, 1})
	f.Fuzz(func(t *testing.T, script []byte) {
		l := newFileLedger("f", noOffset)
		next := int64(1)
		unwritten := map[int64]bool{}
		var inFlight []int64
		for i, op := range script {
			switch op % 4 {
			case 0: // Dispatch the next line.
				l.dispatch(next)
				unwritten[next] = true
				inFlight = append(inFlight, next)
				next++
			case 1: // Acknowledge one in flight, chosen by the next byte.
				if len(inFlight) == 0 {
					continue
				}
				k := 0
				if i+1 < len(script) {
					k = int(script[i+1]) % len(inFlight)
				}
				offset := inFlight[k]
				inFlight = append(inFlight[:k], inFlight[k+1:]...)
				delete(unwritten, offset)
				l.ack(offset)
			case 2: // Hand one back; it stays in flight.
				if len(inFlight) > 0 {
					l.reject(inFlight[0])
				}
			case 3: // Skip the next line as undecodable.
				l.skip(next)
				next++
			}
			l.mu.Lock()
			mark := l.watermarkLocked()
			l.mu.Unlock()
			for offset := range unwritten {
				if offset <= mark {
					t.Fatalf("watermark %d passed unwritten line %d after %v", mark, offset, script[:i+1])
				}
			}
		}
	})
}

// TestAFileTheTableKeepsTurningAwayIsNamedOnce verifies a file whose items the table
// keeps turning away is named on the console, once. A partition that never keeps up
// otherwise shows only as counts that grow, with nothing to say where to look.
func TestAFileTheTableKeepsTurningAwayIsNamedOnce(t *testing.T) {
	out := &testConsole{}
	coord, _ := newTestCoordinator(t, testDeps{console: out})
	l := newFileLedger("stuck-file", noOffset)
	l.dispatch(1)
	it := item{ledger: l, offset: 1}

	for range 2 * stuckAfter {
		coord.settle([]item{it}, writer.Rejection{HandedBack: []int{0}})
		coord.lane.takeDue(time.Now().Add(time.Hour), nil, 1)
		it.attempts++
	}
	if n := strings.Count(out.printed(), "stuck-file"); n != 1 {
		t.Errorf("stuck-file named %d times, want once: %q", n, out.printed())
	}
}
