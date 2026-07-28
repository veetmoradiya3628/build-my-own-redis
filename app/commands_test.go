package main

// import (
// 	"net"
// 	"testing"
// 	"time"
// )

// func TestHandleRPUSHReturnsListLength(t *testing.T) {
// 	client, server := net.Pipe()
// 	defer client.Close()
// 	defer server.Close()

// 	store := NewStore()
// 	done := make(chan struct{})
// 	go func() {
// 		handleRPUSH(server, []interface{}{"RPUSH", "k1", "1", "2", "3"}, store)
// 		close(done)
// 	}()

// 	buf := make([]byte, 32)
// 	n, err := client.Read(buf)
// 	if err != nil {
// 		t.Fatalf("expected RPUSH response, got error: %v", err)
// 	}

// 	if got := string(buf[:n]); got != ":3\r\n" {
// 		t.Fatalf("expected integer reply :3\r\n, got %q", got)
// 	}

// 	select {
// 	case <-done:
// 	case <-time.After(time.Second):
// 		t.Fatal("expected RPUSH handler to finish")
// 	}
// }

// func TestHandleLRangeReturnsListElements(t *testing.T) {
// 	client, server := net.Pipe()
// 	defer client.Close()
// 	defer server.Close()

// 	store := NewStore()
// 	store.RPush("k1", []string{"1", "2", "3"}...)

// 	done := make(chan struct{})
// 	go func() {
// 		handleLRANGE(server, []interface{}{"LRANGE", "k1", "0", "-1"}, store)
// 		close(done)
// 	}()

// 	buf := make([]byte, 64)
// 	n, err := client.Read(buf)
// 	if err != nil {
// 		t.Fatalf("expected LRANGE response, got error: %v", err)
// 	}

// 	expected := "*3\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n"
// 	if got := string(buf[:n]); got != expected {
// 		t.Fatalf("expected array reply %q, got %q", expected, got)
// 	}

// 	select {
// 	case <-done:
// 	case <-time.After(time.Second):
// 		t.Fatal("expected LRANGE handler to finish")
// 	}
// }

// func TestHandleLRangeWithNegativeIndices(t *testing.T) {
// 	client, server := net.Pipe()
// 	defer client.Close()
// 	defer server.Close()

// 	store := NewStore()
// 	store.RPush("k1", []string{"1", "2", "3", "4", "5"}...)

// 	done := make(chan struct{})
// 	go func() {
// 		handleLRANGE(server, []interface{}{"LRANGE", "k1", "-3", "-1"}, store)
// 		close(done)
// 	}()

// 	buf := make([]byte, 64)
// 	n, err := client.Read(buf)
// 	if err != nil {
// 		t.Fatalf("expected LRANGE response, got error: %v", err)
// 	}

// 	expected := "*3\r\n$1\r\n3\r\n$1\r\n4\r\n$1\r\n5\r\n"
// 	if got := string(buf[:n]); got != expected {
// 		t.Fatalf("expected array reply %q, got %q", expected, got)
// 	}

// 	select {
// 	case <-done:
// 	case <-time.After(time.Second):
// 		t.Fatal("expected LRANGE handler to finish")
// 	}
// }

// func TestLPushPreservesRedisOrder(t *testing.T) {
// 	store := NewStore()
// 	store.RPush("strawberry", "raspberry")

// 	store.LPush("strawberry", "grape", "orange")

// 	list, ok := store.Get("strawberry")
// 	if !ok {
// 		t.Fatal("expected list to be stored")
// 	}

// 	values, ok := list.([]string)
// 	if !ok {
// 		t.Fatalf("expected list type []string, got %T", list)
// 	}

// 	want := []string{"orange", "grape", "raspberry"}
// 	if len(values) != len(want) {
// 		t.Fatalf("expected %d values, got %d", len(want), len(values))
// 	}
// 	for i := range want {
// 		if values[i] != want[i] {
// 			t.Fatalf("expected value %d to be %q, got %q", i, want[i], values[i])
// 		}
// 	}
// }

// func TestLLENReturnsCorrectLength(t *testing.T) {
// 	client, server := net.Pipe()
// 	defer client.Close()
// 	defer server.Close()

// 	store := NewStore()
// 	store.RPush("k1", []string{"1", "2", "3"}...)

// 	done := make(chan struct{})
// 	go func() {
// 		handleLLEN(server, []interface{}{"LLEN", "k1"}, store)
// 		close(done)
// 	}()

// 	buf := make([]byte, 32)
// 	n, err := client.Read(buf)
// 	if err != nil {
// 		t.Fatalf("expected LLEN response, got error: %v", err)
// 	}

// 	if got := string(buf[:n]); got != ":3\r\n" {
// 		t.Fatalf("expected integer reply :3\r\n, got %q", got)
// 	}

// 	select {
// 	case <-done:
// 	case <-time.After(time.Second):
// 		t.Fatal("expected LLEN handler to finish")
// 	}
// }

// func TestLLENReturnsZeroForNonExistentKey(t *testing.T) {
// 	client, server := net.Pipe()
// 	defer client.Close()
// 	defer server.Close()

// 	store := NewStore()
// 	done := make(chan struct{})
// 	go func() {
// 		handleLLEN(server, []interface{}{"LLEN", "nonexistent"}, store)
// 		close(done)
// 	}()

// 	buf := make([]byte, 32)
// 	n, err := client.Read(buf)
// 	if err != nil {
// 		t.Fatalf("expected LLEN response, got error: %v", err)
// 	}

// 	if got := string(buf[:n]); got != ":0\r\n" {
// 		t.Fatalf("expected integer reply :0\r\n, got %q", got)
// 	}

// 	select {
// 	case <-done:
// 	case <-time.After(time.Second):
// 		t.Fatal("expected LLEN handler to finish")
// 	}
// }

// func TestLPOPReturnsFirstElement(t *testing.T) {
// 	client, server := net.Pipe()
// 	defer client.Close()
// 	defer server.Close()

// 	store := NewStore()
// 	store.RPush("k1", []string{"1", "2", "3"}...)

// 	done := make(chan struct{})
// 	go func() {
// 		handleLPOP(server, []interface{}{"LPOP", "k1"}, store)
// 		close(done)
// 	}()

// 	buf := make([]byte, 32)
// 	n, err := client.Read(buf)
// 	if err != nil {
// 		t.Fatalf("expected LPOP response, got error: %v", err)
// 	}

// 	if got := string(buf[:n]); got != "$1\r\n1\r\n" {
// 		t.Fatalf("expected bulk string reply $1\r\n1\r\n, got %q", got)
// 	}
// }

// func TestRPOPReturnsLastElement(t *testing.T) {
// 	client, server := net.Pipe()
// 	defer client.Close()
// 	defer server.Close()

// 	store := NewStore()
// 	store.RPush("k1", []string{"1", "2", "3"}...)

// 	done := make(chan struct{})
// 	go func() {
// 		handleRPOP(server, []interface{}{"RPOP", "k1"}, store)
// 		close(done)
// 	}()

// 	buf := make([]byte, 32)
// 	n, err := client.Read(buf)
// 	if err != nil {
// 		t.Fatalf("expected RPOP response, got error: %v", err)
// 	}

// 	if got := string(buf[:n]); got != "$1\r\n3\r\n" {
// 		t.Fatalf("expected bulk string reply $1\r\n3\r\n, got %q", got)
// 	}
// }
