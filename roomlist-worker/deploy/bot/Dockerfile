FROM golang:1.25.13-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY pkg/ pkg/
COPY roomlist-worker/ roomlist-worker/

RUN CGO_ENABLED=0 go build -o /roomlist-worker ./roomlist-worker/

FROM alpine:3.21

RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app

COPY --from=builder /roomlist-worker /roomlist-worker

USER app
ENTRYPOINT ["/roomlist-worker"]
