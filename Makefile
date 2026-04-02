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
	CGO_ENABLED=1 go build -o tmp/swarun ./cmd/swarun/main.go
	CGO_ENABLED=1 go build -o tmp/swarun-condor ./scenarios/condor/main.go

test-go:
	go test ./...

docker-compose-up: docker-build docker-create-network
	docker compose up -d

docker-compose-down:
	docker compose down

docker-build:
	docker build -t swarun:latest .

docker-push-ecr:
	aws ecr get-login-password --region ap-northeast-1 | docker login --username AWS --password-stdin 647655508639.dkr.ecr.ap-northeast-1.amazonaws.com
	docker tag swarun:latest 647655508639.dkr.ecr.ap-northeast-1.amazonaws.com/condor-swarun:latest
	docker push 647655508639.dkr.ecr.ap-northeast-1.amazonaws.com/condor-swarun:latest

docker-create-network:
	docker network inspect swarun_default >/dev/null 2>&1 || \
	docker network create swarun_default

docker-run: docker-build docker-create-network
	docker run --rm -it \
		--name controller \
		--network swarun_default \
		-p 8080:8080 \
		-v $(pwd)/data:/app/data \
		-v /var/run/docker.sock:/var/run/docker.sock \
		-e SWARUN_DATA_DIR=/app/data \
		-e SWARUN_PLATFORM=docker \
		swarun:latest -mode controller

cdk-diff:
	cd cdk && pnpm cdk-diff CondorSwarunCdkStack

cdk-deploy:
	cd cdk && pnpm cdk-deploy CondorSwarunCdkStack

clean:
	rm -rf tmp/
	rm -rf web/dist/
	rm -rf pkg/cli/web/

lint-web:
	cd web && pnpm run check

format-web:
	cd web && pnpm run check:apply
