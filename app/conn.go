package main

import (
	"bufio"
	"fmt"
	"log/slog"
	"net"
)

// writeBulkString writes a RESP Bulk String response to conn.
func writeBulkString(conn net.Conn, s string) error {
	_, err := fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(s), s)
	if err != nil {
		slog.Error("failed to write bulk string", "err", err)
	} else if addr := conn.RemoteAddr(); addr != nil {
		slog.Debug("wrote bulk string", "remote", addr.String(), "len", len(s))
	}
	return err
}

// handleConnection reads RESP values from conn, dispatches commands,
// and writes responses. Logging parse errors helps debugging without
// mixing command logic here.
func handleConnection(conn net.Conn, store *Store, config ServerConfig) {
	defer conn.Close()
	defer store.RemoveSubscriber(conn)
	defer store.ClearTx(conn)
	defer store.Unwatch(conn)

	reader := bufio.NewReader(conn)
	for {
		val, err := ParseRESP(reader)
		if err != nil {
			slog.Error("parse error", "err", err)
			break
		}
		arr, ok := val.([]any)
		if !ok || len(arr) == 0 {
			continue
		}
		handleCommand(conn, arr, store, config)
	}
}
