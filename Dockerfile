# Stage 1: Build the Go binary
FROM golang:1.24-alpine AS builder

WORKDIR /app
# Copy module files and download dependencies (if you have a go.mod)
# COPY go.mod go.sum ./
# RUN go mod download

# Copy the source code 
COPY . .

# Build the Go application
RUN CGO_ENABLED=0 GOOS=linux go build -o redis-clone ./app

# Stage 2: Create the minimal runtime image
FROM alpine:latest

WORKDIR /app

# Create a data directory for RDB and AOF files
RUN mkdir -p /data

# Copy the compiled binary from the builder stage
COPY --from=builder /app/redis-clone .

# Expose the default port
EXPOSE 6379

# Command to run the application using the config file flag
ENTRYPOINT ["./redis-clone"]
CMD ["--config", "/app/redis.conf", "--dir", "/data"]