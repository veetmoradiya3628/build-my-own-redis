#!/bin/sh
set -e

# Ensure we run from the repository root
cd "$(dirname "$0")" || exit 1

# Require `go` to be installed for local builds
if ! command -v go >/dev/null 2>&1; then
  echo "Error: 'go' is not installed or not in PATH. Install Go to build and run locally."
  exit 1
fi

# Build output (can override with BUILD_DIR env var)
BUILD_DIR=${BUILD_DIR:-/tmp}
BIN="$BUILD_DIR/codecrafters-build-redis-go"

echo "Building app/*.go -> $BIN"
go build -o "$BIN" app/*.go

echo "Running $BIN"
exec "$BIN" "$@"