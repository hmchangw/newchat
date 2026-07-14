FROM golang:1.25.12-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY pkg/ pkg/
COPY data-migration/oplog-transformer/ data-migration/oplog-transformer/

RUN CGO_ENABLED=0 go build -o /oplog-transformer ./data-migration/oplog-transformer/

FROM alpine:3.21

RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app

COPY --from=builder /oplog-transformer /oplog-transformer

USER app
ENTRYPOINT ["/oplog-transformer"]
