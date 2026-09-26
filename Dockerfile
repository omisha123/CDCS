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
# storage/config dirs are created via volume mounts at run time, not baked in
ENTRYPOINT ["./cdcs-node"]