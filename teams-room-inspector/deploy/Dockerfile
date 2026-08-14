FROM golang:1.25.13-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY pkg/ pkg/
COPY teams-room-inspector/ teams-room-inspector/
RUN CGO_ENABLED=0 go build -o /teams-room-inspector ./teams-room-inspector/

FROM alpine:3.21
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
COPY --from=builder /teams-room-inspector /teams-room-inspector
USER app
EXPOSE 8080
ENTRYPOINT ["/teams-room-inspector"]
