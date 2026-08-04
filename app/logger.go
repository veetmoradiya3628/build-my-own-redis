package main

import (
    "log/slog"
    "os"
)

func init() {
    // Configure a simple text handler with timestamps for local development.
    handler := slog.NewTextHandler(os.Stderr, nil)
    slog.SetDefault(slog.New(handler))
}
