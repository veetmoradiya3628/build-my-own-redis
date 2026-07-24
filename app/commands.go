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

func writeArrayResponse(conn net.Conn, items []string) error {
	var builder strings.Builder
	builder.WriteString("*")
	builder.WriteString(strconv.Itoa(len(items)))
	builder.WriteString("\r\n")
	for _, item := range items {
		builder.WriteString("$")
		builder.WriteString(strconv.Itoa(len(item)))
		builder.WriteString("\r\n")
		builder.WriteString(item)
		builder.WriteString("\r\n")
	}
	_, err := conn.Write([]byte(builder.String()))
	return err
}

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
	case "LRANGE":
		handleLRANGE(conn, arr, store)
	case "LLEN":
		handleLLEN(conn, arr, store)
	case "LPOP":
		handleLPOP(conn, arr, store)
	case "RPOP":
		handleRPOP(conn, arr, store)
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

func handleLRANGE(conn net.Conn, arr []interface{}, store *Store) {
	if len(arr) < 4 {
		writeErr(conn, "wrong number of arguments for 'LRANGE' command")
		return
	}
	key, _ := asString(arr[1])
	startStr, _ := asString(arr[2])
	endStr, _ := asString(arr[3])
	start, err1 := strconv.Atoi(startStr)
	end, err2 := strconv.Atoi(endStr)
	if err1 != nil || err2 != nil {
		writeErr(conn, "LRANGE start and end must be integers")
		return
	}
	if v, ok := store.Get(key); ok {
		if list, ok := v.([]string); ok {
			length := len(list)
			if start < 0 {
				start = length + start
			}
			if end < 0 {
				end = length + end
			}
			if start < 0 {
				start = 0
			}
			if end >= length {
				end = length - 1
			}
			if start > end || start >= length {
				conn.Write([]byte("*0\r\n"))
				return
			}
			sublist := list[start : end+1]
			if err := writeArrayResponse(conn, sublist); err != nil {
				return
			}
		} else {
			writeErr(conn, "LRANGE key is not a list")
		}
	} else {
		conn.Write([]byte("*0\r\n"))
	}
}

func handleLLEN(conn net.Conn, arr []interface{}, store *Store) {
	if len(arr) < 2 {
		writeErr(conn, "wrong number of arguments for 'LLEN' command")
		return
	}
	key, _ := asString(arr[1])
	if v, ok := store.Get(key); ok {
		if list, ok := v.([]string); ok {
			length := len(list)
			_, _ = conn.Write([]byte(":" + strconv.Itoa(length) + "\r\n"))
		} else {
			writeErr(conn, "LLEN key is not a list")
		}
	} else {
		_, _ = conn.Write([]byte(":0\r\n"))
	}
}

func handleLPOP(conn net.Conn, arr []interface{}, store *Store) {
	if len(arr) < 2 {
		writeErr(conn, "wrong number of arguments for 'LPOP' command")
		return
	}
	key, _ := asString(arr[1])
	cnt := 1
	if len(arr) == 3 {
		cnt, _ = strconv.Atoi(arr[2].(string))
	}
	if v, ok := store.Get(key); ok {
		if list, ok := v.([]string); ok {
			if len(list) == 0 {
				writeNullBulk(conn)
				return
			}
			if cnt <= 0 {
				writeErr(conn, "LPOP count must be a positive integer")
				return
			}
			if cnt > len(list) {
				cnt = len(list)
			}
			poppedValues := list[:cnt]
			store.Set(key, list[cnt:])
			if len(poppedValues) == 1 {
				writeBulkString(conn, poppedValues[0])
			} else {
				writeArrayResponse(conn, poppedValues)
			}
		} else {
			writeErr(conn, "LPOP key is not a list")
		}
	} else {
		writeNullBulk(conn)
	}
}

func handleRPOP(conn net.Conn, arr []interface{}, store *Store) {
	if len(arr) < 2 {
		writeErr(conn, "wrong number of arguments for 'RPOP' command")
		return
	}
	key, _ := asString(arr[1])
	if v, ok := store.Get(key); ok {
		if list, ok := v.([]string); ok {
			if len(list) == 0 {
				writeNullBulk(conn)
				return
			}
			poppedValue := list[len(list)-1]
			store.Set(key, list[:len(list)-1])
			writeBulkString(conn, poppedValue)
		} else {
			writeErr(conn, "RPOP key is not a list")
		}
	} else {
		writeNullBulk(conn)
	}
}
