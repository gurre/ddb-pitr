package checkpoint

import (
	"context"
	"sync"
)

// MemoryStore implements the Store interface using memory storage.
// It's primarily intended for testing purposes.
type MemoryStore struct {
	state State
	mu    sync.RWMutex
}

// NewMemoryStore creates a new MemoryStore instance
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{}
}

// Load retrieves the current checkpoint state from memory
func (s *MemoryStore) Load(ctx context.Context) (State, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state, nil
}

// Save stores the checkpoint state in memory
func (s *MemoryStore) Save(ctx context.Context, state State) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = state
	return nil
}
