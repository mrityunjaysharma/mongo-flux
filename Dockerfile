FROM golang:1.22-alpine AS builder

RUN apk add --no-cache git ca-certificates

WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o mongoflux ./cmd/mongoflux

# --- Production runtime image ---
FROM alpine:3.19

RUN apk add --no-cache ca-certificates tini curl \
    && addgroup -S mongoflux && adduser -S mongoflux -G mongoflux

COPY --from=builder /build/mongoflux /usr/local/bin/mongoflux
COPY config/mongoflux.yaml /etc/mongoflux/mongoflux.yaml

RUN mkdir -p /var/lib/mongoflux/resume_tokens \
    && chown -R mongoflux:mongoflux /var/lib/mongoflux

USER mongoflux
EXPOSE 9090

ENTRYPOINT ["tini", "--", "/usr/local/bin/mongoflux"]
CMD ["/etc/mongoflux/mongoflux.yaml"]

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -sf http://localhost:9090/health || exit 1
