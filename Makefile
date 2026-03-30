.PHONY: all build gen-proto gen-web-proto build-web build-go clean docker-up docker-down docker-build lint-web format-web

all: gen-proto gen-web-proto build-web build-go

gen-proto:
	buf generate proto --template buf.gen.yaml

gen-web-proto:
	cd web && npx buf generate ../proto --template buf.gen.web.yaml

build-web: gen-web-proto
	cd web && pnpm install && pnpm run build
	mkdir -p pkg/cli/web/dist
	cp -r web/dist/* pkg/cli/web/dist/

build-go:
	mkdir -p tmp
	go build -o tmp/swarun ./cmd/swarun/main.go
	go build -o tmp/swarun-example ./examples/simple-get/main.go

docker-build:
	docker build -t swarun:latest .

docker-up:
	docker compose up -d

docker-down:
	docker compose down

clean:
	rm -rf tmp/
	rm -rf web/dist/
	rm -rf pkg/cli/web/

lint-web:
	cd web && pnpm run check

format-web:
	cd web && pnpm run check:apply
