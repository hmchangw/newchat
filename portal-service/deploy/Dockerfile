FROM golang:1.25.12-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY pkg/ pkg/
COPY portal-service/ portal-service/
RUN CGO_ENABLED=0 go build -o /portal-service ./portal-service/

FROM alpine:3.21
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
COPY --from=builder /portal-service /portal-service
USER app
ENTRYPOINT ["/portal-service"]
