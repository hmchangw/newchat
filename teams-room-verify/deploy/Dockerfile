FROM golang:1.25.12-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY pkg/ pkg/
COPY teams-room-verify/ teams-room-verify/
RUN CGO_ENABLED=0 go build -o /teams-room-verify ./teams-room-verify/

FROM alpine:3.21
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
COPY --from=builder /teams-room-verify /teams-room-verify
USER app
ENTRYPOINT ["/teams-room-verify"]
