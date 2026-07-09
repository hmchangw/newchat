FROM golang:1.26.5-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY pkg/ pkg/
COPY message-gatekeeper/ message-gatekeeper/
RUN CGO_ENABLED=0 go build -o /message-gatekeeper ./message-gatekeeper/

FROM alpine:3.21
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
COPY --from=builder /message-gatekeeper /message-gatekeeper
USER app
ENTRYPOINT ["/message-gatekeeper"]
