FROM golang:1.25.12-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY pkg/ pkg/
COPY teams-chat-member-sync/ teams-chat-member-sync/
RUN CGO_ENABLED=0 go build -o /teams-chat-member-sync ./teams-chat-member-sync/

FROM alpine:3.21
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
COPY --from=builder /teams-chat-member-sync /teams-chat-member-sync
USER app
ENTRYPOINT ["/teams-chat-member-sync"]
