FROM golang:1.26.5-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY pkg/ pkg/
COPY user-presence-service/ user-presence-service/
RUN CGO_ENABLED=0 go build -o /user-presence-service ./user-presence-service/

FROM alpine:3.21
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
COPY --from=builder /user-presence-service /user-presence-service
USER app
ENTRYPOINT ["/user-presence-service"]
