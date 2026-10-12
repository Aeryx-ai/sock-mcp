VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -X main.version=$(VERSION)

.DEFAULT_GOAL := build
.PHONY: build test install redeploy check lint fmt vet

build:
	go build -ldflags "$(LDFLAGS)" -o bin/sock-mcp ./cmd/sock-mcp

test:
	go test -race ./...

install:
	go install -ldflags "$(LDFLAGS)" ./cmd/sock-mcp

redeploy:
	@echo "sock-mcp has nothing to deploy; run 'make install' to update the local binary"

lint:
	golangci-lint run

fmt:
	@out="$$(gofmt -l .)"; if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

vet:
	go vet ./...

check: fmt vet lint test
