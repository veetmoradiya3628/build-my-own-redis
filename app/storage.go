package main

import (
	"errors"
	"net"
	"sort"
	"strconv"
	"sync"
	"time"
)

type Store struct {
	cache               map[string]any
	mu                  sync.RWMutex
	waiters             map[string][]chan string
	expiry              map[string]time.Time
	pubsub              map[string]map[net.Conn]struct{}
	clientSubscriptions map[net.Conn]map[string]struct{}
	transactions 		map[net.Conn][][]any
}

type ZSetNode struct {
	member string
	score  float64
}

func NewStore(data map[string]any, expiry map[string]time.Time) *Store {
	if data == nil {
		data = make(map[string]any)
	}
	if expiry == nil {
		expiry = make(map[string]time.Time)
	}

	return &Store{
		cache:               data,
		expiry:              expiry, // Initialize with RDB expiry data
		waiters:             make(map[string][]chan string),
		pubsub:              make(map[string]map[net.Conn]struct{}),
		clientSubscriptions: make(map[net.Conn]map[string]struct{}),
		transactions:        make(map[net.Conn][][]any),
	}
}

// Set stores a key/value pair without expiration.
func (s *Store) Set(key string, value any) {
	s.mu.Lock()
	s.cache[key] = value
	delete(s.expiry, key)
	s.mu.Unlock()
}

// SetWithTTL stores a key/value pair and deletes it after ttlMillis milliseconds.
func (s *Store) SetWithTTL(key string, value any, ttlMillis int) {
	s.Set(key, value)

	// Calculate and store the expiration time in our new map
	s.mu.Lock()
	expirationTime := time.Now().Add(time.Duration(ttlMillis) * time.Millisecond)
	s.expiry[key] = expirationTime
	s.mu.Unlock()

	// active cleanup goroutine, but fix the locking and add a safety check
	go func() {
		time.Sleep(time.Duration(ttlMillis) * time.Millisecond)

		s.mu.Lock()
		defer s.mu.Unlock()

		// Double-check that the key hasn't been updated with a NEW expiration time
		// while this goroutine was sleeping
		if expTime, exists := s.expiry[key]; exists && !time.Now().Before(expTime) {
			delete(s.cache, key)
			delete(s.expiry, key)
		}
	}()
}

// Get returns the value and whether it exists.
func (s *Store) Get(key string) (any, bool) {
	s.mu.RLock()
	v, ok := s.cache[key]
	expTime, hasExpiry := s.expiry[key]
	s.mu.RUnlock()

	// If the key exists and has an expiration, check if it's expired
	if ok && hasExpiry {
		if time.Now().After(expTime) {
			// Key has expired, actively delete it and return false
			s.mu.Lock()
			delete(s.cache, key)
			delete(s.expiry, key)
			s.mu.Unlock()
			return nil, false
		}
	}
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

// helper function to clean up expired channel for the key from waiters map
func (s *Store) CleanUpExpiredKeyWaiter(key string, waiterChan chan string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if queues, exists := s.waiters[key]; exists {
		for i, ch := range queues {
			if ch == waiterChan {
				// Remove this specific channel
				s.waiters[key] = append(queues[:i], queues[i+1:]...)
				break
			}
		}
		// Clean up the key if the queue is now empty
		if len(s.waiters[key]) == 0 {
			delete(s.waiters, key)
		}
	}
}

// Keys
func (s *Store) Keys(pattern string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var keys []string

	// Iterate through the cache map
	for k := range s.cache {
		// For this stage, we only care about the "*" pattern
		if pattern == "*" {
			keys = append(keys, k)
		}
	}
	return keys
}

func (s *Store) ZAdd(key string, score float64, member string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	added := 0
	existing, ok := s.cache[key]
	var zset map[string]float64

	if !ok {
		zset = make(map[string]float64)
		s.cache[key] = zset
	} else {
		var isZset bool
		if zset, isZset = existing.(map[string]float64); !isZset {
			return 0
		}
	}

	if _, exists := zset[member]; !exists {
		added = 1
	}
	zset[member] = score
	return added
}

// util function
func (s *Store) getSortedZSetNodes(key string) []ZSetNode {
	s.mu.RLock()
	defer s.mu.RUnlock()

	existing, ok := s.cache[key]
	if !ok {
		return []ZSetNode{}
	}

	zset, isZset := existing.(map[string]float64)
	if !isZset {
		return []ZSetNode{}
	}

	nodes := make([]ZSetNode, 0, len(zset))
	for member, score := range zset {
		nodes = append(nodes, ZSetNode{member: member, score: score})
	}

	sort.Slice(nodes, func(i, j int) bool {
		// If scores are equal, sort lexicographically
		if nodes[i].score == nodes[j].score {
			return nodes[i].member < nodes[j].member
		}
		// Otherwise, sort by lowest score to highest
		return nodes[i].score < nodes[j].score
	})

	return nodes
}

func (s *Store) ZRange(key string, start, stop int) []string {

	nodes := s.getSortedZSetNodes(key)
	if len(nodes) == 0 {
		return []string{}
	}

	length := len(nodes)
	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}

	if start < 0 {
		start = 0
	}
	if stop >= length {
		stop = length - 1
	}
	if start > stop || start >= length {
		return []string{}
	}

	var result []string
	for i := start; i <= stop; i++ {
		result = append(result, nodes[i].member)
	}

	return result
}

func (s *Store) ZRank(key, member string) int {
	nodes := s.getSortedZSetNodes(key)

	for i, n := range nodes {
		if n.member == member {
			return i
		}
	}

	return -1
}

func (s *Store) ZCard(key string) int {
	existing, ok := s.cache[key]
	if !ok {
		return 0
	}

	zset, isZset := existing.(map[string]float64)
	if !isZset {
		return 0
	}
	return len(zset)
}

func (s *Store) getZscoreValue(key, member string) (float64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	existing, ok := s.cache[key]
	if !ok {
		return 0, false
	}

	zset, isZset := existing.(map[string]float64)
	if !isZset {
		return 0, false
	}

	score, found := zset[member]
	return score, found
}

// ZRem removes specified members from the sorted set at key.
// It returns the number of members actually removed.
func (s *Store) ZRem(key string, members []string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing, ok := s.cache[key]
	if !ok {
		return 0
	}

	zset, isZset := existing.(map[string]float64)
	if !isZset {
		return 0
	}

	removedCount := 0
	for _, member := range members {
		if _, exists := zset[member]; exists {
			delete(zset, member)
			removedCount++
		}
	}

	if len(zset) == 0 {
		delete(s.cache, key)
	}

	return removedCount
}

func (s *Store) Subscribe(conn net.Conn, channel string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.pubsub[channel] == nil {
		s.pubsub[channel] = make(map[net.Conn]struct{})
	}
	s.pubsub[channel][conn] = struct{}{}

	if s.clientSubscriptions[conn] == nil {
		s.clientSubscriptions[conn] = make(map[string]struct{})
	}
	s.clientSubscriptions[conn][channel] = struct{}{}

	return len(s.clientSubscriptions[conn])
}

func (s *Store) Unsubscribe(conn net.Conn, channel string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if subscribers, exists := s.pubsub[channel]; exists {
		delete(subscribers, conn)
		if len(subscribers) == 0 {
			delete(s.pubsub, channel)
		}
	}

	if channels, exists := s.clientSubscriptions[conn]; exists {
		delete(channels, channel)
		remainingCount := len(channels)
		if remainingCount == 0 {
			delete(s.clientSubscriptions, conn)
		}
		return remainingCount
	}
	return 0
}

func (s *Store) RemoveSubscriber(conn net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, subscribers := range s.pubsub {
		delete(subscribers, conn)
	}
}

func (s *Store) IsSubscribed(conn net.Conn) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	channels, exists := s.clientSubscriptions[conn]
	return exists && len(channels) > 0
}

func (s *Store) PublishMessageOnChannel(channel, message string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	subscribers, exists := s.pubsub[channel]

	if !exists {
		return 0
	}

	// publish to subscribers
	for subscriber := range subscribers {
		go func(subscriber net.Conn) {
			writeArrayResponse(subscriber, []string{"message", channel, message})
		}(subscriber)
	}

	return len(subscribers)
}

func (s *Store) Incr(key string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing, ok := s.cache[key]
	if !ok {
		// Key doesn't exist, set to 1 directly
		s.cache[key] = "1"
		return 1, nil
	}

	// Key exists, ensure it's a string since handleSET stores values as strings
	strVal, isString := existing.(string)
	if !isString {
		return 0, errors.New("invalid type")
	}

	// Parse the string to an integer
	intVal, err := strconv.Atoi(strVal)
	if err != nil {
		return 0, errors.New("not an integer")
	}

	// Increment and store back as a string
	intVal++
	s.cache[key] = strconv.Itoa(intVal)

	return intVal, nil
}
// Transaction commands
// StartTx marks a connection as being in a transaction block.
// Returns false if the connection is already in a transaction.
func (s *Store) StartTx(conn net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if _, exists := s.transactions[conn]; exists {
		return false // Already in a transaction
	}
	
	// Initialize an empty queue for this connection
	s.transactions[conn] = make([][]any, 0)
	return true
}

// QueueCommand adds a command array to the connection's transaction queue.
func (s *Store) QueueCommand(conn net.Conn, arr []any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if queue, exists := s.transactions[conn]; exists {
		s.transactions[conn] = append(queue, arr)
	}
}

// IsInTx checks if a connection is currently in a transaction block.
func (s *Store) IsInTx(conn net.Conn) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	_, exists := s.transactions[conn]
	return exists
}

// ClearTx removes the transaction state for a connection.
func (s *Store) ClearTx(conn net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	delete(s.transactions, conn)
}