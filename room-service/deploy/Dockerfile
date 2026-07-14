FROM golang:1.25.12-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY pkg/ pkg/
COPY room-service/ room-service/
RUN CGO_ENABLED=0 go build -o /room-service ./room-service/

FROM alpine:3.21
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
COPY --from=builder /room-service /room-service
USER app
ENTRYPOINT ["/room-service"]
