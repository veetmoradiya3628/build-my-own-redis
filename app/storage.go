package main

import (
	"sync"
	"time"
)

var (
	cache   = map[string]string{}
	cacheMu sync.RWMutex
)

// Set stores a key/value pair without expiration.
func Set(key, value string) {
	cacheMu.Lock()
	cache[key] = value
	cacheMu.Unlock()
}

// SetWithTTL stores a key/value pair and deletes it after ttlMillis milliseconds.
func SetWithTTL(key, value string, ttlMillis int) {
	Set(key, value)
	go func() {
		time.Sleep(time.Duration(ttlMillis) * time.Millisecond)
		cacheMu.Lock()
		delete(cache, key)
		cacheMu.Unlock()
	}()
}

// Get returns the value and whether it exists.
func Get(key string) (string, bool) {
	cacheMu.RLock()
	v, ok := cache[key]
	cacheMu.RUnlock()
	return v, ok
}
