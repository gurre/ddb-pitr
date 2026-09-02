package checkpoint

import (
	"context"
	"sync"
)

// MemoryStore implements the Store interface in memory. It backs a run that asked for
// no checkpoint URI and every dry run: progress lives only as long as the process, so
// an interrupted run starts over.
type MemoryStore struct {
	state State
	mu    sync.RWMutex
}

// NewMemoryStore creates a new MemoryStore instance
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{}
}

// Load returns a copy of the current state. Handing out the stored slice and map
// would let a caller's later edits change what the store holds.
func (s *MemoryStore) Load(ctx context.Context) (State, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return copyState(s.state), nil
}

// Save stores a copy of the checkpoint state in memory
func (s *MemoryStore) Save(ctx context.Context, state State) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = copyState(state)
	return nil
}

// copyState duplicates the slice and map a State carries.
func copyState(state State) State {
	out := State{ExportID: state.ExportID}
	if state.Completed != nil {
		out.Completed = append([]string(nil), state.Completed...)
	}
	if state.Offsets != nil {
		out.Offsets = make(map[string]int64, len(state.Offsets))
		for key, offset := range state.Offsets {
			out.Offsets[key] = offset
		}
	}
	return out
}
