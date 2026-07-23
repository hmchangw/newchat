FROM golang:1.25.12-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY pkg/ pkg/
COPY bot-msg-handler/ bot-msg-handler/
RUN CGO_ENABLED=0 go build -o /bot-msg-handler ./bot-msg-handler/

FROM alpine:3.21
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
COPY --from=builder /bot-msg-handler /bot-msg-handler
USER app
ENTRYPOINT ["/bot-msg-handler"]
