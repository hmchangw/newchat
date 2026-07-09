FROM golang:1.26.5-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY pkg/ pkg/
COPY data-migration/oplog-direct-transfer/ data-migration/oplog-direct-transfer/

RUN CGO_ENABLED=0 go build -o /oplog-direct-transfer ./data-migration/oplog-direct-transfer/

FROM alpine:3.21

RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app

COPY --from=builder /oplog-direct-transfer /oplog-direct-transfer

USER app
ENTRYPOINT ["/oplog-direct-transfer"]
