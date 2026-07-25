package main

import (
	"sync"
	"time"
)

type Store struct {
	cache   map[string]any
	mu      sync.RWMutex
	waiters map[string][]chan string
}

func NewStore() *Store {
	return &Store{
		cache:   make(map[string]any),
		waiters: make(map[string][]chan string),
	}
}

// Set stores a key/value pair without expiration.
func (s *Store) Set(key string, value any) {
	s.mu.Lock()
	s.cache[key] = value
	s.mu.Unlock()
}

// SetWithTTL stores a key/value pair and deletes it after ttlMillis milliseconds.
func (s *Store) SetWithTTL(key string, value any, ttlMillis int) {
	s.Set(key, value)
	go func() {
		time.Sleep(time.Duration(ttlMillis) * time.Millisecond)
		s.mu.Lock()
		delete(s.cache, key)
		s.mu.Unlock()
	}()
}

// Get returns the value and whether it exists.
func (s *Store) Get(key string) (any, bool) {
	s.mu.RLock()
	v, ok := s.cache[key]
	s.mu.RUnlock()
	return v, ok
}

// LPush prepends values to a list stored at key. If the key does not exist, it creates a new list.
func (s *Store) LPush(key string, values ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.cache[key]; ok {
		if list, ok := existing.([]string); ok {
			newList := make([]string, 0, len(values)+len(list))
			for i := len(values) - 1; i >= 0; i-- {
				newList = append(newList, values[i])
			}
			newList = append(newList, list...)
			s.cache[key] = newList
			return len(newList)
		}
	}

	reversed := make([]string, len(values))
	for i, v := range values {
		reversed[len(values)-1-i] = v
	}
	s.cache[key] = reversed
	return len(reversed)
}

// RPush appends values to a list stored at key. If the key does not exist, it creates a new list.
func (s *Store) RPush(key string, values ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.cache[key]; ok {
		if list, ok := existing.([]string); ok {
			newList := append(list, values...)
			s.cache[key] = newList
			return len(newList)
		}
	}
	s.cache[key] = values
	return len(values)
}
