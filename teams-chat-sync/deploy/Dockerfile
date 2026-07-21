FROM golang:1.25.12-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY pkg/ pkg/
COPY teams-chat-sync/ teams-chat-sync/
RUN CGO_ENABLED=0 go build -o /teams-chat-sync ./teams-chat-sync/

FROM alpine:3.21
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
COPY --from=builder /teams-chat-sync /teams-chat-sync
USER app
ENTRYPOINT ["/teams-chat-sync"]
