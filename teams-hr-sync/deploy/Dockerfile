FROM golang:1.25.12-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY pkg/ pkg/
COPY teams-hr-sync/ teams-hr-sync/
RUN CGO_ENABLED=0 go build -o /teams-hr-sync ./teams-hr-sync/

FROM alpine:3.21
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
COPY --from=builder /teams-hr-sync /teams-hr-sync
USER app
ENTRYPOINT ["/teams-hr-sync"]
