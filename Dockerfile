FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY app ./app
RUN go build -o /out/redis-server ./app

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=build /out/redis-server /usr/local/bin/redis-server
EXPOSE 6379
ENTRYPOINT ["/usr/local/bin/redis-server"]
