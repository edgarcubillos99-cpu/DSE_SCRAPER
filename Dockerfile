# Build
FROM golang:1.25-alpine AS builder

WORKDIR /app

ENV GOTOOLCHAIN=local

RUN apk add --no-cache ca-certificates git

COPY go.mod go.sum ./
RUN go mod download

COPY main.go realtime.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /dse-scraper-api .

# Runtime
FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

COPY --from=builder /dse-scraper-api .

EXPOSE 8080

USER nobody

CMD ["./dse-scraper-api"]
