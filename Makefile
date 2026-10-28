# MiniRedis Makefile
#
# Every target runs a real command: `make check` is exactly the sequence the
# README and the CI workflow document. Nothing here is a placeholder.

GO      ?= go
BINDIR  ?= bin
PKGDIR  := github.com/amandev11/miniredis

SERVER_BIN := $(BINDIR)/miniredis
CLI_BIN    := $(BINDIR)/mini-cli
BENCH_BIN  := $(BINDIR)/mini-bench

.DEFAULT_GOAL := build

.PHONY: all build test race vet fmt fmt-check tidy run cli bench bench-cli \
        integration demo cover check clean docker-build docker-run help

all: build

## build: compile the server, the client and the load generator into bin/
build:
	@mkdir -p $(BINDIR)
	$(GO) build -trimpath -o $(SERVER_BIN) ./cmd/miniredis
	$(GO) build -trimpath -o $(CLI_BIN) ./cmd/mini-cli
	$(GO) build -trimpath -o $(BENCH_BIN) ./cmd/mini-bench
	@echo "built: $(SERVER_BIN) $(CLI_BIN) $(BENCH_BIN)"

## test: run the whole test suite
test:
	$(GO) test ./...

## race: run the whole test suite under the race detector
race:
	$(GO) test -race ./...

## vet: run go vet on every package
vet:
	$(GO) vet ./...

## fmt: format the source tree
fmt:
	$(GO) fmt ./...

## fmt-check: fail if any file is not gofmt'd
fmt-check:
	@out="$$(gofmt -l .)"; \
	if [ -n "$$out" ]; then \
		echo "these files are not gofmt'd:"; echo "$$out"; exit 1; \
	fi; \
	echo "gofmt: clean"

## tidy: verify go.mod/go.sum are tidy
tidy:
	$(GO) mod tidy
	@git diff --exit-code -- go.mod go.sum >/dev/null 2>&1 || \
		echo "note: go.mod or go.sum changed; commit the update"

## run: start the server with the default configuration
run:
	$(GO) run ./cmd/miniredis

## cli: start an interactive client against 127.0.0.1:6379
cli:
	$(GO) run ./cmd/mini-cli

## bench: run the Go benchmarks (they start their own server on loopback)
bench:
	$(GO) test -run '^$$' -bench . -benchmem ./internal/server

## bench-cli: run the standalone load generator against a running server
bench-cli: build
	$(BENCH_BIN) -addr 127.0.0.1:6379 -test SET -n 200000 -c 50 -p 16

## integration: run only the end-to-end tests, under the race detector
integration:
	$(GO) test -race -count=1 ./tests/...

## demo: build, then run the end-to-end demonstration script
demo: build
	./scripts/demo.sh

## cover: write a coverage profile and print the total
cover:
	$(GO) test -covermode=atomic -coverprofile=coverage.out ./...
	$(GO) tool cover -func=coverage.out | tail -1

## check: everything CI runs before a merge
check: fmt-check vet build test race integration

## clean: remove build output and local profiling artifacts
clean:
	rm -rf $(BINDIR) coverage.out cpu.prof mem.prof

## docker-build: build the container image
docker-build:
	docker build -t miniredis:local .

## docker-run: run the container with a persistent volume
docker-run:
	docker run --rm -p 127.0.0.1:6379:6379 -p 127.0.0.1:7379:7379 \
		-e MINIREDIS_REQUIREPASS="$${MINIREDIS_PASSWORD:?set MINIREDIS_PASSWORD first}" \
		-v miniredis-data:/data miniredis:local

## help: list the targets
help:
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/^## //' | awk -F: '{printf "  make %-14s %s\n", $$1, substr($$0, index($$0,$$2))}'
