FROM golang:1.25 AS builder

RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libc6-dev \
    make

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# DuckDB を使用するため CGO を有効にする
RUN CGO_ENABLED=1 GOOS=linux go build -o /usr/local/bin/swarun ./cmd/swarun/main.go
RUN CGO_ENABLED=1 GOOS=linux go build -o /app/tmp/swarun-example ./examples/simple-get/main.go

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /usr/local/bin/swarun /usr/local/bin/swarun
COPY --from=builder /app/tmp/swarun-example /usr/local/bin/swarun-example

# ランタイムに必要なディレクトリ
RUN mkdir -p /app/data

ENTRYPOINT ["swarun-example"]
