FROM golang:1.25.12-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY pkg/ pkg/
COPY push-service/ push-service/

RUN CGO_ENABLED=0 go build -o /push-service ./push-service/

FROM alpine:3.21

RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app

COPY --from=builder /push-service /push-service

USER app
ENTRYPOINT ["/push-service"]
