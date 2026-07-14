FROM golang:1.25.11-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY pkg/ pkg/
COPY botplatform-service/ botplatform-service/
RUN CGO_ENABLED=0 go build -o /botplatform-service ./botplatform-service/

FROM alpine:3.21
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
COPY --from=builder /botplatform-service /botplatform-service
USER app
ENTRYPOINT ["/botplatform-service"]
