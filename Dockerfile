# ---- Build stage ----
FROM golang:1.26 AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o cdcs-node .

# ---- Runtime stage ----
FROM alpine:3.20
WORKDIR /app
COPY --from=builder /app/cdcs-node .
COPY index.html .
# Run as a non-root user. /app must be writable (orchestrator creates
# metadata.db here); /app/storage and /app/raft are volume mounts, so the
# host folders (./data/*) must be owned by uid 10001:
#   sudo chown -R 10001:10001 data
RUN adduser -D -u 10001 cdcs && mkdir -p /app/storage /app/raft && chown -R cdcs:cdcs /app
USER cdcs
# storage/config dirs are created via volume mounts at run time, not baked in
ENTRYPOINT ["./cdcs-node"]