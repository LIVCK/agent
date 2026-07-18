.PHONY: help build loadgen dist package test test-race e2e soak vet lint fmt tidy clean wire

# livck-agent build. The binary is pure-static (CGO disabled) and stripped, as
# the hardened systemd unit expects.
BINARY_NAME := livck-agent
# VERSION defaults to the latest git tag (leading 'v' stripped so deb/rpm accept
# it); empty (no tags) falls back to 0.0.0. Override for releases: make package VERSION=1.4.0
VERSION     ?= $(shell git describe --tags --abbrev=0 2>/dev/null | sed 's/^v//')
ifeq ($(strip $(VERSION)),)
VERSION     := 0.0.0
endif
GIT_COMMIT  := $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
LDFLAGS     := -s -w -X main.version=$(VERSION)
GOFLAGS     := -trimpath

DIST_DIR  := dist
ARCHES    := amd64 arm64
PACKAGERS := deb rpm

help: ## Show this help
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-12s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: ## Build the agent binary (static, stripped) for the host arch
	CGO_ENABLED=0 go build $(GOFLAGS) -ldflags="$(LDFLAGS)" -o $(BINARY_NAME) ./cmd/livck-agent

loadgen: ## Build the fake-fleet load generator (dev tool; live pulse load test)
	CGO_ENABLED=0 go build $(GOFLAGS) -o livck-loadgen ./cmd/livck-loadgen

dist: ## Cross-build stripped static binaries + sha256 for amd64+arm64 into dist/
	@mkdir -p $(DIST_DIR)
	@for arch in $(ARCHES); do \
		echo ">> build $(BINARY_NAME)_linux_$$arch ($(VERSION))"; \
		CGO_ENABLED=0 GOOS=linux GOARCH=$$arch go build $(GOFLAGS) -ldflags="$(LDFLAGS)" \
			-o $(DIST_DIR)/$(BINARY_NAME)_linux_$$arch ./cmd/livck-agent || exit 1; \
		( cd $(DIST_DIR) && sha256sum $(BINARY_NAME)_linux_$$arch > $(BINARY_NAME)_linux_$$arch.sha256 ); \
	done

package: dist ## Build deb+rpm for amd64+arm64 via nfpm (build-time tool; run from repo root)
	@command -v nfpm >/dev/null 2>&1 || { echo "nfpm not found. Install: go install github.com/goreleaser/nfpm/v2/cmd/nfpm@latest"; exit 1; }
	@mkdir -p $(DIST_DIR)
	@for arch in $(ARCHES); do \
		cp $(DIST_DIR)/$(BINARY_NAME)_linux_$$arch $(DIST_DIR)/$(BINARY_NAME).pkgbin; \
		for pkg in $(PACKAGERS); do \
			echo ">> package $$pkg/$$arch ($(VERSION))"; \
			ARCH=$$arch VERSION=$(VERSION) nfpm package -f packaging/nfpm.yaml -p $$pkg -t $(DIST_DIR)/ || exit 1; \
		done; \
	done
	@rm -f $(DIST_DIR)/$(BINARY_NAME).pkgbin
	@echo ">> artifacts:"; ls -1 $(DIST_DIR)/*.deb $(DIST_DIR)/*.rpm 2>/dev/null || true

test: ## Run tests
	go test ./...

test-race: ## Run tests with the race detector
	go test -race ./...

e2e: ## Run the hermetic run-loop e2e gates under the race detector
	go test -race -run 'TestE2E|TestBackfillReplay' -v ./internal/runner/

soak: ## Run the reproducible memory soak (override: make soak DURATION=24h)
	LIVCK_SOAK_DURATION=$(if $(DURATION),$(DURATION),5m) go test -run '^TestSoakMemoryFlat$$' -v -timeout 0 ./internal/runner/

vet: ## Run go vet
	go vet ./...

lint: ## Run golangci-lint
	golangci-lint run

fmt: ## Format the code
	gofmt -s -w .

tidy: ## Tidy module dependencies
	go mod tidy

wire: ## Build and test the frozen wire contract module
	cd pkg/wire && go build ./... && go test ./...

clean: ## Remove build output
	rm -rf $(BINARY_NAME) coverage.out coverage.html $(DIST_DIR)
