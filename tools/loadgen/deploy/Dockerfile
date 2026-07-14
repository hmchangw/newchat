FROM golang:1.25.12-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY pkg/ pkg/
COPY tools/loadgen/ tools/loadgen/

RUN CGO_ENABLED=0 go build -o /loadgen ./tools/loadgen/

FROM alpine:3.21
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
COPY --from=builder /loadgen /loadgen
USER app
ENTRYPOINT ["/loadgen"]
