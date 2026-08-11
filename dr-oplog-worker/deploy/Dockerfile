FROM golang:1.25.12-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY pkg/ pkg/
COPY dr-oplog-worker/ dr-oplog-worker/

RUN CGO_ENABLED=0 go build -o /dr-oplog-worker ./dr-oplog-worker/

FROM alpine:3.21

RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app

COPY --from=builder /dr-oplog-worker /dr-oplog-worker

USER app
ENTRYPOINT ["/dr-oplog-worker"]
