# Stage 1: Web Builder
FROM node:22-slim AS web-builder
WORKDIR /app/web
RUN corepack enable && corepack prepare pnpm@latest --activate
COPY web/package.json web/pnpm-lock.yaml ./
RUN pnpm install
COPY web/ .
# web/src/gen などの生成物は事前にホスト側で生成されていることを前提とする
# もしコンテナ内で生成したい場合は buf のインストールが必要
RUN pnpm run build

# Stage 2: Go Builder
FROM golang:1.25 AS go-builder

RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libc6-dev \
    make

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
# Web Builder でビルドした成果物を Go 側の埋め込み対象ディレクトリにコピー
RUN mkdir -p pkg/cli/web/dist
COPY --from=web-builder /app/web/dist/ pkg/cli/web/dist/

# DuckDB を使用するため CGO を有効にする
RUN CGO_ENABLED=1 GOOS=linux go build -o /usr/local/bin/swarun ./cmd/swarun/main.go
RUN CGO_ENABLED=1 GOOS=linux go build -o /app/tmp/swarun-condor ./scenarios/condor/main.go

# Stage 3: Runtime
FROM debian:trixie-slim
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=go-builder /usr/local/bin/swarun /usr/local/bin/swarun
COPY --from=go-builder /app/tmp/swarun-condor /usr/local/bin/swarun-condor

# ランタイムに必要なディレクトリ
RUN mkdir -p /app/data

ENTRYPOINT ["swarun-condor"]
