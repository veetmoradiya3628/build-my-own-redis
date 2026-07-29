package main

import (
	"bufio"
	"fmt"
	"net"
)

// writeBulkString writes a RESP Bulk String response to conn.
func writeBulkString(conn net.Conn, s string) error {
	_, err := fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(s), s)
	return err
}

// handleConnection reads RESP values from conn, dispatches commands,
// and writes responses. Logging parse errors helps debugging without
// mixing command logic here.
func handleConnection(conn net.Conn, store *Store, config ServerConfig) {
	defer conn.Close()
	defer store.RemoveSubscriber(conn)

	reader := bufio.NewReader(conn)
	for {
		val, err := ParseRESP(reader)
		if err != nil {
			fmt.Println("parse error:", err)
			break
		}
		arr, ok := val.([]any)
		if !ok || len(arr) == 0 {
			continue
		}
		handleCommand(conn, arr, store, config)
	}
}
