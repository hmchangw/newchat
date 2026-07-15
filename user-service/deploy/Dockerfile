FROM golang:1.26.5-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY pkg/ pkg/
COPY user-service/ user-service/
RUN CGO_ENABLED=0 go build -o /user-service ./user-service/

FROM alpine:3.21
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
COPY --from=builder /user-service /user-service
USER app
ENTRYPOINT ["/user-service"]
