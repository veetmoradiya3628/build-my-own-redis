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
