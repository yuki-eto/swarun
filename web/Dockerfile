FROM node:22-slim

WORKDIR /app

RUN corepack enable && corepack prepare pnpm@latest --activate

# package.json と pnpm-lock.yaml をコピー
COPY package.json ./

# 依存関係をインストール
RUN pnpm install

# ソースコードをコピー (ボリュームマウントするため、開発時は不要だが、イメージ単体でも動くように)
COPY . .

# Vite の開発サーバーを起動 (ホットリロード対応)
CMD ["pnpm", "dev", "--host"]
