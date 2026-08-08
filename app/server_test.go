package main

import (
	"net"
	"testing"
	"time"
)

func TestReplicaHandshakeSendsRESPPing(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer listener.Close()

	received := make(chan string, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		buf := make([]byte, 64)
		n, err := conn.Read(buf)
		if err != nil {
			return
		}
		received <- string(buf[:n])
	}()

	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatalf("split host port failed: %v", err)
	}

	if err := performReplicaHandshake("127.0.0.1", port); err != nil {
		t.Fatalf("performReplicaHandshake failed: %v", err)
	}

	select {
	case got := <-received:
		want := "*1\r\n$4\r\nPING\r\n"
		if got != want {
			t.Fatalf("unexpected handshake payload: got %q want %q", got, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for replica handshake")
	}
}
