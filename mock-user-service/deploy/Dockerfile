FROM golang:1.25.8-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY pkg/ pkg/
COPY mock-user-service/ mock-user-service/

RUN CGO_ENABLED=0 go build -o /mock-user-service ./mock-user-service/

FROM alpine:3.21

RUN apk add --no-cache ca-certificates

COPY --from=builder /mock-user-service /mock-user-service

ENTRYPOINT ["/mock-user-service"]
