package main

import (
	"net"
	"strconv"
	"strings"
)

// Helper: safely assert interface{} to string
func asString(v interface{}) (string, bool) {
	s, ok := v.(string)
	return s, ok
}

// Helper: write common replies
func writeOK(conn net.Conn)              { conn.Write([]byte("+OK\r\n")) }
func writeErr(conn net.Conn, msg string) { conn.Write([]byte("-ERR " + msg + "\r\n")) }
func writeNullBulk(conn net.Conn)        { conn.Write([]byte("$-1\r\n")) }

// parseTTL checks for EX/PX options in SET arguments and returns
// ttl in milliseconds and true if present and valid.
func parseTTL(arr []interface{}) (int, bool) {
	if len(arr) < 5 {
		return 0, false
	}
	optRaw, ok := asString(arr[3])
	if !ok {
		return 0, false
	}
	opt := strings.ToUpper(optRaw)
	if opt != "EX" && opt != "PX" {
		return 0, false
	}
	ttlStr, ok := asString(arr[4])
	if !ok {
		return 0, false
	}
	ttl, err := strconv.Atoi(ttlStr)
	if err != nil {
		return 0, false
	}
	if opt == "EX" {
		ttl = ttl * 1000 // convert seconds to milliseconds
	}
	return ttl, true
}

// handleCommand dispatches a parsed RESP array (as []interface{}) to
// the appropriate command handler. It writes responses directly to conn.
func handleCommand(conn net.Conn, arr []interface{}, store *Store) {
	if len(arr) == 0 {
		return
	}
	cmdRaw, ok := asString(arr[0])
	if !ok {
		return
	}
	cmd := strings.ToUpper(cmdRaw)
	switch cmd {
	case "PING":
		handlePING(conn, arr)
	case "ECHO":
		handleECHO(conn, arr)
	case "SET":
		handleSET(conn, arr, store)
	case "GET":
		handleGET(conn, arr, store)
	case "LPUSH":
		handleLPUSH(conn, arr, store)
	case "RPUSH":
		handleRPUSH(conn, arr, store)
	default:
		writeErr(conn, "unknown command")
	}
}

// handlePING replies with PONG for a PING command.
func handlePING(conn net.Conn, arr []interface{}) {
	if len(arr) == 1 {
		conn.Write([]byte("+PONG\r\n"))
	}
}

// handleECHO replies with the provided argument as a bulk string.
func handleECHO(conn net.Conn, arr []interface{}) {
	if len(arr) < 2 {
		writeNullBulk(conn)
		return
	}
	if arg, ok := asString(arr[1]); ok {
		writeBulkString(conn, arg)
	} else {
		writeNullBulk(conn)
	}
}

// handleSET stores a key/value pair and supports optional EX/PX TTL.
func handleSET(conn net.Conn, arr []interface{}, store *Store) {
	if len(arr) < 3 {
		writeErr(conn, "wrong number of arguments for 'SET' command")
		return
	}
	key, _ := asString(arr[1])
	value, _ := asString(arr[2])
	if ttl, ok := parseTTL(arr); ok {
		store.SetWithTTL(key, value, ttl)
		writeOK(conn)
		return
	}
	store.Set(key, value)
	writeOK(conn)
}

// handleGET retrieves a key and returns it as a bulk string or null.
func handleGET(conn net.Conn, arr []interface{}, store *Store) {
	if len(arr) < 2 {
		writeErr(conn, "wrong number of arguments for 'GET' command")
		return
	}
	key, _ := asString(arr[1])
	if v, ok := store.Get(key); ok {
		writeBulkString(conn, v.(string))
	} else {
		writeNullBulk(conn)
	}
}

func handleLPUSH(conn net.Conn, arr []interface{}, store *Store) {
	if len(arr) < 3 {
		writeErr(conn, "wrong number of arguments for 'LPUSH' command")
		return
	}
	key, _ := asString(arr[1])
	values := make([]string, 0, len(arr)-2)
	for _, v := range arr[2:] {
		if str, ok := asString(v); ok {
			values = append(values, str)
		} else {
			writeErr(conn, "LPUSH values must be strings")
			return
		}
	}
	length := store.LPush(key, values...)
	_, _ = conn.Write([]byte(":" + strconv.Itoa(length) + "\r\n"))
}

func handleRPUSH(conn net.Conn, arr []interface{}, store *Store) {
	if len(arr) < 3 {
		writeErr(conn, "wrong number of arguments for 'RPUSH' command")
		return
	}
	key, _ := asString(arr[1])
	values := make([]string, 0, len(arr)-2)
	for _, v := range arr[2:] {
		if str, ok := asString(v); ok {
			values = append(values, str)
		} else {
			writeErr(conn, "RPUSH values must be strings")
			return
		}
	}
	length := store.RPush(key, values...)
	_, _ = conn.Write([]byte(":" + strconv.Itoa(length) + "\r\n"))
}
