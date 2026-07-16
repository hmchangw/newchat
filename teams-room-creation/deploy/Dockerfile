FROM golang:1.25.12-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY pkg/ pkg/
COPY teams-room-creation/ teams-room-creation/
RUN CGO_ENABLED=0 go build -o /teams-room-creation ./teams-room-creation/

FROM alpine:3.21
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
COPY --from=builder /teams-room-creation /teams-room-creation
USER app
ENTRYPOINT ["/teams-room-creation"]
