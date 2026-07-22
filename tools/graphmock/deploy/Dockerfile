FROM golang:1.25.12-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY pkg/ pkg/
COPY tools/graphmock/ tools/graphmock/
RUN CGO_ENABLED=0 go build -o /graphmock ./tools/graphmock/

FROM alpine:3.21
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
COPY --from=builder /graphmock /graphmock
COPY tools/graphmock/fixtures.sample.json /fixtures.sample.json
USER app
ENTRYPOINT ["/graphmock"]
