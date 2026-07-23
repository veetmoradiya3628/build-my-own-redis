package main

import (
	"net"
	"testing"
	"time"
)

func TestHandleRPUSHReturnsListLength(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	store := NewStore()
	done := make(chan struct{})
	go func() {
		handleRPUSH(server, []interface{}{"RPUSH", "k1", "1", "2", "3"}, store)
		close(done)
	}()

	buf := make([]byte, 32)
	n, err := client.Read(buf)
	if err != nil {
		t.Fatalf("expected RPUSH response, got error: %v", err)
	}

	if got := string(buf[:n]); got != ":3\r\n" {
		t.Fatalf("expected integer reply :3\r\n, got %q", got)
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("expected RPUSH handler to finish")
	}
}

func TestHandleLRangeReturnsListElements(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	store := NewStore()
	store.RPush("k1", []string{"1", "2", "3"}...)

	done := make(chan struct{})
	go func() {
		handleLRANGE(server, []interface{}{"LRANGE", "k1", "0", "-1"}, store)
		close(done)
	}()

	buf := make([]byte, 64)
	n, err := client.Read(buf)
	if err != nil {
		t.Fatalf("expected LRANGE response, got error: %v", err)
	}

	expected := "*3\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n"
	if got := string(buf[:n]); got != expected {
		t.Fatalf("expected array reply %q, got %q", expected, got)
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("expected LRANGE handler to finish")
	}
}

func TestHandleLRangeWithNegativeIndices(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	store := NewStore()
	store.RPush("k1", []string{"1", "2", "3", "4", "5"}...)

	done := make(chan struct{})
	go func() {
		handleLRANGE(server, []interface{}{"LRANGE", "k1", "-3", "-1"}, store)
		close(done)
	}()

	buf := make([]byte, 64)
	n, err := client.Read(buf)
	if err != nil {
		t.Fatalf("expected LRANGE response, got error: %v", err)
	}

	expected := "*3\r\n$1\r\n3\r\n$1\r\n4\r\n$1\r\n5\r\n"
	if got := string(buf[:n]); got != expected {
		t.Fatalf("expected array reply %q, got %q", expected, got)
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("expected LRANGE handler to finish")
	}
}
