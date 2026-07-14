FROM golang:1.25.12-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY pkg/ pkg/
COPY data-migration/oplog-connector/ data-migration/oplog-connector/

RUN CGO_ENABLED=0 go build -o /oplog-connector ./data-migration/oplog-connector/

FROM alpine:3.21

RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app

COPY --from=builder /oplog-connector /oplog-connector

USER app
ENTRYPOINT ["/oplog-connector"]
