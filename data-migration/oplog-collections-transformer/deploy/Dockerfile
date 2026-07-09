FROM golang:1.26.5-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY pkg/ pkg/
COPY data-migration/oplog-collections-transformer/ data-migration/oplog-collections-transformer/

RUN CGO_ENABLED=0 go build -o /oplog-collections-transformer ./data-migration/oplog-collections-transformer/

FROM alpine:3.21

RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app

COPY --from=builder /oplog-collections-transformer /oplog-collections-transformer

USER app
ENTRYPOINT ["/oplog-collections-transformer"]
