FROM golang:1.25.13-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY pkg/ pkg/
COPY tools/clientsim/ tools/clientsim/

RUN CGO_ENABLED=0 go build -o /clientsim ./tools/clientsim/

FROM alpine:3.21
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
COPY --from=builder /clientsim /clientsim
USER app
ENTRYPOINT ["/clientsim"]
