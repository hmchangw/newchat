FROM golang:1.25.12-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY pkg/ pkg/
COPY auth-service/ auth-service/
RUN CGO_ENABLED=0 go build -o /auth-service ./auth-service/

FROM alpine:3.21
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
COPY --from=builder /auth-service /auth-service
USER app
ENTRYPOINT ["/auth-service"]
