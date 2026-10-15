.PHONY: all build test race vet fmt bench cover cluster-up cluster-down docker-up docker-down loadtest

all: fmt vet test

build:
	go build -o bin/ ./cmd/...

test:
	go test ./...

race:
	go test -race ./...

vet:
	go vet ./...

fmt:
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

bench:
	go test -run '^$$' -bench . -benchmem ./internal/...

cover:
	go test -coverprofile=coverage.out ./... && go tool cover -func=coverage.out | tail -1

cluster-up:
	scripts/local-cluster.sh up

cluster-down:
	scripts/local-cluster.sh down

docker-up:
	scripts/dev-config.sh
	docker compose -f deploy/docker-compose.yml up -d --build

docker-down:
	docker compose -f deploy/docker-compose.yml --profile expansion down

loadtest:
	go run ./cmd/loadgen -workload mixed -size small -keys 2000 -concurrency 16 -duration 30s
