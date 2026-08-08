package main

import (
	"fmt"
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

	received := make(chan []string, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		var payloads []string
		for i := 0; i < 3; i++ {
			buf := make([]byte, 256)
			n, err := conn.Read(buf)
			if err != nil {
				return
			}
			payloads = append(payloads, string(buf[:n]))

			response := "+PONG\r\n"
			if i > 0 {
				response = "+OK\r\n"
			}
			if _, err := conn.Write([]byte(response)); err != nil {
				return
			}
		}
		received <- payloads
	}()

	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatalf("split host port failed: %v", err)
	}

	if err := performReplicaHandshake("127.0.0.1", port, "6380"); err != nil {
		t.Fatalf("performReplicaHandshake failed: %v", err)
	}

	select {
	case got := <-received:
		wants := []string{
			"*1\r\n$4\r\nPING\r\n",
			fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$%d\r\n6380\r\n", len("6380")),
			"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n",
		}
		if len(got) != len(wants) {
			t.Fatalf("unexpected handshake payload count: got %d want %d", len(got), len(wants))
		}
		for i, want := range wants {
			if got[i] != want {
				t.Fatalf("unexpected handshake payload %d: got %q want %q", i, got[i], want)
			}
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for replica handshake")
	}
}
