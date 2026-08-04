package main

import (
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"time"
)

// Helper: safely assert any to string
func asString(v any) (string, bool) {
	s, ok := v.(string)
	return s, ok
}

// Helper: write common replies
func writeOK(conn net.Conn) {
	_, _ = conn.Write([]byte("+OK\r\n"))
	if addr := conn.RemoteAddr(); addr != nil {
		slog.Debug("writeOK", "remote", addr.String())
	}
}

func writeErr(conn net.Conn, msg string) {
	_, _ = conn.Write([]byte("-ERR " + msg + "\r\n"))
	if addr := conn.RemoteAddr(); addr != nil {
		slog.Warn("writeErr", "remote", addr.String(), "msg", msg)
	}
}

func writeNullBulk(conn net.Conn) {
	_, _ = conn.Write([]byte("$-1\r\n"))
	if addr := conn.RemoteAddr(); addr != nil {
		slog.Debug("writeNullBulk", "remote", addr.String())
	}
}

func writeInteger(conn net.Conn, value int) {
	_, _ = conn.Write([]byte(":" + strconv.Itoa(value) + "\r\n"))
	if addr := conn.RemoteAddr(); addr != nil {
		slog.Debug("writeInteger", "remote", addr.String(), "value", value)
	}
}

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
	if err != nil {
		slog.Error("failed to write array response", "err", err)
	} else {
		if addr := conn.RemoteAddr(); addr != nil {
			slog.Debug("wrote array response", "remote", addr.String(), "count", len(items))
		}
	}
	return err
}

// parseTTL checks for EX/PX options in SET arguments and returns
// ttl in milliseconds and true if present and valid.
func parseTTL(arr []any) (int, bool) {
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

// handleCommand dispatches a parsed RESP array (as []any) to
// the appropriate command handler. It writes responses directly to conn.
func handleCommand(conn net.Conn, arr []any, store *Store, config ServerConfig) {
	if len(arr) == 0 {
		return
	}
	cmdRaw, ok := asString(arr[0])
	if !ok {
		return
	}
	cmd := strings.ToUpper(cmdRaw)

	// Log the incoming command for debugging/tracing.
	remote := "unknown"
	if addr := conn.RemoteAddr(); addr != nil {
		remote = addr.String()
	}
	slog.Debug("handleCommand", "remote", remote, "raw_cmd", cmd)

	// Intercept commands if the client is in Subscribed mode
	if store.IsSubscribed(conn) {
		switch cmd {
		case "SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE", "PING", "QUIT", "RESET":
		default:
			errMsg := "Can't execute '" + strings.ToLower(cmd) + "': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"
			writeErr(conn, errMsg)
			return
		}
	}

	// Intercept commands if the client is in a Transaction (MULTI) block
	if store.IsInTx(conn) {
		switch cmd {
		case "EXEC", "DISCARD", "MULTI", "QUIT", "WATCH":
			// Let these commands pass through to be handled normally
		default:
			// Queue the command and return +QUEUED
			store.QueueCommand(conn, arr)
			conn.Write([]byte("+QUEUED\r\n"))
			return
		}
	}

	switch cmd {
	case "PING":
		handlePING(conn, arr, store)
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
	case "BLPOP":
		handleBLPOP(conn, arr, store)
	case "CONFIG":
		handleCONFIG(conn, arr, config)
	case "KEYS":
		handleKEYS(conn, arr, store)
	case "ZADD":
		handleZADD(conn, arr, store)
	case "ZRANGE":
		handleZRANGE(conn, arr, store)
	case "ZRANK":
		handleZRANK(conn, arr, store)
	case "ZCARD":
		handleZCARD(conn, arr, store)
	case "ZSCORE":
		handleZSCORE(conn, arr, store)
	case "ZREM":
		handleZREM(conn, arr, store)
	case "SUBSCRIBE":
		handleSUBSCRIBE(conn, arr, store)
	case "PUBLISH":
		handlePUBLISH(conn, arr, store)
	case "UNSUBSCRIBE":
		handleUNSUBSCRIBE(conn, arr, store)
	case "INCR":
		handleINCR(conn, arr, store)
	case "MULTI":
		handleMULTI(conn, arr, store)
	case "EXEC":
		handleEXEC(conn, arr, store, config)
	case "DISCARD":
		handleDISCARD(conn, arr, store)
	case "WATCH":
		handleWATCH(conn, arr, store)
	case "UNWATCH":
		handleUNWATCH(conn, arr, store)
	default:
		slog.Warn("unknown command", "cmd", cmd, "remote", remote)
		writeErr(conn, "unknown command")
	}
}

// handlePING replies with PONG for a PING command.
func handlePING(conn net.Conn, arr []any, store *Store) {
	if len(arr) == 1 {
		if store.IsSubscribed(conn) {
			writeArrayResponse(conn, []string{"pong", ""})
		} else {
			conn.Write([]byte("+PONG\r\n"))
		}
	}
}

// handleECHO replies with the provided argument as a bulk string.
func handleECHO(conn net.Conn, arr []any) {
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
func handleSET(conn net.Conn, arr []any, store *Store) {
	if len(arr) < 3 {
		writeErr(conn, "wrong number of arguments for 'SET' command")
		return
	}
	key, _ := asString(arr[1])
	value, _ := asString(arr[2])
	if ttl, ok := parseTTL(arr); ok {
		slog.Debug("SET with TTL", "key", key, "ttl_ms", ttl)
		store.SetWithTTL(key, value, ttl)
		writeOK(conn)
		return
	}
	slog.Debug("SET", "key", key)
	store.Set(key, value)
	writeOK(conn)
}

// handleGET retrieves a key and returns it as a bulk string or null.
func handleGET(conn net.Conn, arr []any, store *Store) {
	if len(arr) < 2 {
		writeErr(conn, "wrong number of arguments for 'GET' command")
		return
	}
	key, _ := asString(arr[1])
	if v, ok := store.Get(key); ok {
		slog.Debug("GET hit", "key", key)
		writeBulkString(conn, v.(string))
	} else {
		slog.Debug("GET miss", "key", key)
		writeNullBulk(conn)
	}
}

func handleLPUSH(conn net.Conn, arr []any, store *Store) {
	if len(arr) < 3 {
		writeErr(conn, "wrong number of arguments for 'LPUSH' command")
		return
	}
	key, _ := asString(arr[1])
	slog.Debug("LPUSH", "key", key)
	values := make([]string, 0, len(arr)-2)
	for _, v := range arr[2:] {
		if str, ok := asString(v); ok {
			values = append(values, str)
		} else {
			writeErr(conn, "LPUSH values must be strings")
			return
		}
	}

	var initialLen int
	if v, ok := store.Get(key); ok {
		if list, ok := v.([]string); ok {
			initialLen = len(list)
		}
	}
	expectedLen := initialLen + len(values)

	store.mu.Lock()
	var remaining []string
	for _, val := range values {
		if queues, exists := store.waiters[key]; exists && len(queues) > 0 {
			waiterChan := queues[0]
			store.waiters[key] = queues[1:]

			if len(store.waiters[key]) == 0 {
				delete(store.waiters, key)
			}
			waiterChan <- val
		} else {
			remaining = append(remaining, val)
		}
	}
	store.mu.Unlock()

	if len(remaining) > 0 {
		store.LPush(key, remaining...)
	}

	_, _ = conn.Write([]byte(":" + strconv.Itoa(expectedLen) + "\r\n"))
}

func handleRPUSH(conn net.Conn, arr []any, store *Store) {
	if len(arr) < 3 {
		writeErr(conn, "wrong number of arguments for 'RPUSH' command")
		return
	}
	key, _ := asString(arr[1])
	slog.Debug("RPUSH", "key", key)
	values := make([]string, 0, len(arr)-2)
	for _, v := range arr[2:] {
		if str, ok := asString(v); ok {
			values = append(values, str)
		} else {
			writeErr(conn, "RPUSH values must be strings")
			return
		}
	}

	var initialLen int
	if v, ok := store.Get(key); ok {
		if list, ok := v.([]string); ok {
			initialLen = len(list)
		}
	}
	expectedLen := initialLen + len(values)

	store.mu.Lock()
	var remaining []string
	for _, val := range values {
		if queues, exists := store.waiters[key]; exists && len(queues) > 0 {
			waiterChan := queues[0]
			store.waiters[key] = queues[1:]

			if len(store.waiters[key]) == 0 {
				delete(store.waiters, key)
			}
			waiterChan <- val
		} else {
			remaining = append(remaining, val)
		}
	}
	store.mu.Unlock()

	if len(remaining) > 0 {
		store.RPush(key, remaining...)
	}

	_, _ = conn.Write([]byte(":" + strconv.Itoa(expectedLen) + "\r\n"))
}

func handleLRANGE(conn net.Conn, arr []any, store *Store) {
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

func handleLLEN(conn net.Conn, arr []any, store *Store) {
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

func handleLPOP(conn net.Conn, arr []any, store *Store) {
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

func handleRPOP(conn net.Conn, arr []any, store *Store) {
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

// timeout is always 0 as of now so clean up logic not added for waiters
func handleBLPOP(conn net.Conn, arr []any, store *Store) {
	if len(arr) < 3 {
		writeErr(conn, "wrong number of arguments for 'BLPOP' command")
		return
	}

	key, _ := asString(arr[1])
	timeoutStr, _ := asString(arr[len(arr)-1])

	slog.Debug("BLPOP", "key", key, "timeout", timeoutStr)

	timeoutSeconds, err := strconv.ParseFloat(timeoutStr, 64)
	if err != nil {
		writeErr(conn, "timeout is not a float or integer")
		return
	}

	store.mu.Lock()
	if list, exists := store.cache[key].([]string); exists && len(list) > 0 {
		val := list[0]
		store.cache[key] = list[1:]
		if len(store.cache[key].([]string)) == 0 {
			delete(store.cache, key)
		}
		
		store.markDirty(key) // Notify watches for list pops!

		writeArrayResponse(conn, []string{key, val})
		store.mu.Unlock()
		return
	}

	waiterChan := make(chan string, 1)
	store.waiters[key] = append(store.waiters[key], waiterChan)
	store.mu.Unlock()

	if timeoutSeconds > 0 {
		// Wait for either the value or the timeout
		select {
		case val := <-waiterChan:
			writeArrayResponse(conn, []string{key, val})
		case <-time.After(time.Duration(timeoutSeconds * float64(time.Second))):
			// On timeout, return a null array
			conn.Write([]byte("*-1\r\n"))

			// Clean up the expired channel from the waiters map
			store.CleanUpExpiredKeyWaiter(key, waiterChan)
		}
	} else {
		// Timeout is 0: wait infinitely
		val := <-waiterChan
		writeArrayResponse(conn, []string{key, val})
	}
}

func handleCONFIG(conn net.Conn, arr []any, config ServerConfig) {
	if len(arr) < 2 {
		writeErr(conn, "wrong number of arguments for 'CONFIG' command")
		return // Added return to prevent out-of-bounds panics
	}

	if cmd, ok := asString(arr[1]); ok {
		// It's good practice to make the command case-insensitive
		switch strings.ToUpper(cmd) {
		case "GET":
			if len(arr) < 3 {
				writeErr(conn, "wrong number of arguments for 'CONFIG GET' command")
				return
			}

			param, ok := asString(arr[2])
			if !ok {
				writeErr(conn, "invalid parameter format")
				return
			}

			var value string
			// Match parameter (Redis parameter names are generally case-insensitive)
			switch strings.ToLower(param) {
			case "dir":
				value = config.dir
			case "dbfilename":
				value = config.dbfilename
			default:
				value = ""
			}

			writeArrayResponse(conn, []string{param, value})
		default:
			writeErr(conn, "unknown CONFIG subcommand")
		}
	}
}

// handleKEYS returns all keys matching a pattern. Currently only supports "*".
func handleKEYS(conn net.Conn, arr []any, store *Store) {
	if len(arr) < 2 {
		writeErr(conn, "wrong number of arguments for 'KEYS' command")
		return
	}

	pattern, ok := asString(arr[1])
	if !ok {
		writeErr(conn, "invalid pattern format")
		return
	}

	// We only support the "*" pattern for this stage
	if pattern == "*" {
		// Call the thread-safe Keys method on the store
		keys := store.Keys(pattern)

		// Use your existing helper to write the array response
		writeArrayResponse(conn, keys)
	} else {
		// Return an empty RESP array for unsupported patterns
		conn.Write([]byte("*0\r\n"))
	}
}

func handleZADD(conn net.Conn, arr []any, store *Store) {
	if len(arr) < 4 || len(arr)%2 != 0 {
		writeErr(conn, "wrong number of arguments for 'ZADD' command")
		return
	}

	key, _ := asString(arr[1])
	addedCount := 0

	for i := 2; i < len(arr); i += 2 {
		scoreStr, _ := asString(arr[i])
		member, _ := asString(arr[i+1])

		// Parse the score into a float64
		score, err := strconv.ParseFloat(scoreStr, 64)
		if err != nil {
			writeErr(conn, "ERR value is not a valid float")
			return
		}

		addedCount += store.ZAdd(key, score, member)
	}
	writeInteger(conn, addedCount)
}

func handleZRANGE(conn net.Conn, arr []any, store *Store) {
	if len(arr) < 4 {
		writeErr(conn, "wrong number of arguments for 'ZRANGE' command")
		return
	}

	key, _ := asString(arr[1])
	startStr, _ := asString(arr[2])
	stopStr, _ := asString(arr[3])

	// Parse indices
	start, err1 := strconv.Atoi(startStr)
	stop, err2 := strconv.Atoi(stopStr)
	if err1 != nil || err2 != nil {
		writeErr(conn, "ERR value is not an integer or out of range")
		return
	}

	result := store.ZRange(key, start, stop)
	writeArrayResponse(conn, result)
}

func handleZRANK(conn net.Conn, arr []any, store *Store) {
	if len(arr) < 3 {
		writeErr(conn, "Wrong number of arguments for 'ZRANK' command")
		return
	}

	key, _ := asString(arr[1])
	member, _ := asString(arr[2])

	rank := store.ZRank(key, member)
	if rank != -1 {
		writeInteger(conn, rank)
	} else {
		writeNullBulk(conn)
	}
}

func handleZCARD(conn net.Conn, arr []any, store *Store) {
	if len(arr) < 1 {
		writeErr(conn, "Wrong number of arguments for 'ZCARD' command")
		return
	}

	key, _ := asString(arr[1])
	cnt := store.ZCard(key)
	writeInteger(conn, cnt)
}

func handleZSCORE(conn net.Conn, arr []any, store *Store) {
	if len(arr) < 3 {
		writeErr(conn, "Wrong number of arguments for 'ZSCORE' command")
		return
	}

	key, _ := asString(arr[1])
	member, _ := asString(arr[2])
	value, found := store.getZscoreValue(key, member)
	if !found {
		writeNullBulk(conn)
		return
	}
	writeBulkString(conn, strconv.FormatFloat(value, 'f', -1, 64))
}

func handleZREM(conn net.Conn, arr []any, store *Store) {
	// ZREM needs at least 3 arguments: ZREM, key, and at least one member
	if len(arr) < 3 {
		writeErr(conn, "wrong number of arguments for 'ZREM' command")
		return
	}

	key, _ := asString(arr[1])

	members := make([]string, 0, len(arr)-2)
	for i := 2; i < len(arr); i++ {
		if member, ok := asString(arr[i]); ok {
			members = append(members, member)
		}
	}

	// Execute removal and get the count
	removedCount := store.ZRem(key, members)
	writeInteger(conn, removedCount)
}

func handleSUBSCRIBE(conn net.Conn, arr []any, store *Store) {
	if len(arr) < 2 {
		writeErr(conn, "wrong number of arguments for 'SUBSCRIBE' command")
		return
	}

	for i := 1; i < len(arr); i++ {
		channel, ok := asString(arr[i])
		if !ok {
			continue
		}

		subCount := store.Subscribe(conn, channel)
		slog.Info("SUBSCRIBE", "remote", conn.RemoteAddr(), "channel", channel, "count", subCount)

		// format : *3\r\n$9\r\nsubscribe\r\n$<len(channel)>\r\n<channel>\r\n:<i>\r\n
		resp := fmt.Sprintf("*3\r\n$9\r\nsubscribe\r\n$%d\r\n%s\r\n:%d\r\n", len(channel), channel, subCount)
		conn.Write([]byte(resp))
	}
}

func handleUNSUBSCRIBE(conn net.Conn, arr []any, store *Store) {
	if len(arr) < 2 {
		writeErr(conn, "wrong number of arguments for 'UNSUBSCRIBE' command")
		return
	}

	for i := 1; i < len(arr); i++ {
		channel, ok := asString(arr[i])
		if !ok {
			continue
		}

		subCount := store.Unsubscribe(conn, channel)
		slog.Info("UNSUBSCRIBE", "remote", conn.RemoteAddr(), "channel", channel, "count", subCount)

		// format : *3\r\n$11\r\nunsubscribe\r\n$<len(channel)>\r\n<channel>\r\n:<i>\r\n
		resp := fmt.Sprintf("*3\r\n$11\r\nunsubscribe\r\n$%d\r\n%s\r\n:%d\r\n", len(channel), channel, subCount)
		conn.Write([]byte(resp))
	}
}

func handlePUBLISH(conn net.Conn, arr []any, store *Store) {
	if len(arr) < 2 {
		writeErr(conn, "wrong number of arguments for 'PUBLISH' command")
		return
	}

	channelName, _ := asString(arr[1])
	messageContent, _ := asString(arr[2])

	slog.Info("PUBLISH", "channel", channelName, "message", messageContent)
	publishCnt := store.PublishMessageOnChannel(channelName, messageContent)
	writeInteger(conn, publishCnt)
}
func handleINCR(conn net.Conn, arr []any, store *Store) {
	if len(arr) < 2 {
		writeErr(conn, "wrong number of arguments for 'INCR' command")
		return
	}
	key, _ := asString(arr[1])

	val, err := store.Incr(key)
	if err != nil {
		// Standard Redis error for invalid integer operations
		writeErr(conn, "value is not an integer or out of range")
		return
	}

	slog.Debug("INCR result", "key", key, "value", val)
	writeInteger(conn, val)
}
func handleMULTI(conn net.Conn, arr []any, store *Store) {
	if len(arr) != 1 {
		writeErr(conn, "wrong number of arguments for 'MULTI' command")
		return
	}
	
	// Attempt to start the transaction
	if !store.StartTx(conn) {
		slog.Warn("MULTI nested attempt", "remote", conn.RemoteAddr())
		writeErr(conn, "ERR MULTI calls can not be nested")
		return
	}

	slog.Debug("MULTI started", "remote", conn.RemoteAddr())
	writeOK(conn)
}
// handleEXEC executes queued commands.
// For this stage, it only handles the error case when MULTI hasn't been called.
func handleEXEC(conn net.Conn, arr []any, store *Store, config ServerConfig) {
	if len(arr) != 1 {
		writeErr(conn, "wrong number of arguments for 'EXEC' command")
		return
	}
	queue, wasInTx := store.GetAndClearTx(conn)

	// Check if the connection is currently in a transaction block
	if !wasInTx {
		slog.Warn("EXEC without MULTI", "remote", conn.RemoteAddr())
		writeErr(conn, "EXEC without MULTI")
		return
	}

	isDirty := store.IsDirty(conn)
	store.Unwatch(conn) // EXEC clears watched keys unconditionally

	// If keys were modified, abort transaction and return a RESP null array
	if isDirty {
		slog.Debug("EXEC aborted due to modified watched keys", "remote", conn.RemoteAddr())
		conn.Write([]byte("*-1\r\n")) 
		return
	}

	if len(queue) == 0 {
		slog.Debug("EXEC empty queue", "remote", conn.RemoteAddr())
		conn.Write([]byte("*0\r\n"))
		return
	}

	conn.Write([]byte("*" + strconv.Itoa(len(queue)) + "\r\n"))
	for _, cmdArr := range queue {
		handleCommand(conn, cmdArr, store, config)
	}
}

func handleDISCARD(conn net.Conn, arr []any, store *Store){
	if len(arr) != 1 {
		writeErr(conn, "wrong number of arguments for 'DISCARD' command")
		return
	}

	if !store.IsInTx(conn) {
		slog.Warn("DISCARD without MULTI", "remote", conn.RemoteAddr())
		writeErr(conn, "DISCARD without MULTI")
		return
	}

	store.ClearTx(conn)
	store.Unwatch(conn) // DISCARD also clears watched keys
	slog.Debug("DISCARD executed", "remote", conn.RemoteAddr())
	writeOK(conn)
}

func handleWATCH(conn net.Conn, arr []any, store *Store) {
	if len(arr) < 2 {
		writeErr(conn, "wrong number of arguments for 'WATCH' command")
		return
	}
	if store.IsInTx(conn) {
		writeErr(conn, "WATCH inside MULTI is not allowed")
		return
	}

	keys := make([]string, 0, len(arr)-1)
	for _, v := range arr[1:] {
		if key, ok := asString(v); ok {
			keys = append(keys, key)
		}
	}

	store.Watch(conn, keys)
	writeOK(conn)
}

func handleUNWATCH(conn net.Conn, arr []any, store *Store) {
	if len(arr) != 1 {
		writeErr(conn, "wrong number of arguments for 'UNWATCH' command")
		return
	}
	store.Unwatch(conn)
	writeOK(conn)
}