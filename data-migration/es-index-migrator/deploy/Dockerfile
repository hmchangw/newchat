FROM golang:1.25.12-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY pkg/ pkg/
COPY data-migration/es-index-migrator/ data-migration/es-index-migrator/

RUN CGO_ENABLED=0 go build -o /es-index-migrator ./data-migration/es-index-migrator/

FROM alpine:3.21

RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app

COPY --from=builder /es-index-migrator /es-index-migrator

USER app
ENTRYPOINT ["/es-index-migrator"]
