FROM golang:1.26.1-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /tmp/outbox-publisher ./outbox-publisher/cmd

FROM alpine:latest
RUN apk --no-cache add ca-certificates

WORKDIR /root/

COPY --from=builder /tmp/outbox-publisher ./outbox-publisher

ENTRYPOINT ["./outbox-publisher"]