# All tooling lives here. `make help` lists targets.

GO := go

# So installed tools run even when GOPATH/bin is not on PATH.
export PATH := $(shell $(GO) env GOPATH)/bin:$(PATH)

.PHONY: help
help: ## list available targets
	@grep -hE '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "%-13s %s\n", $$1, $$2}'

.PHONY: run
run: ## build with -race and run the server
	$(GO) build -race && ./ws-chat

# No -race: it more than doubles the CPU per delivery, and needs cgo.
.PHONY: build
build: generate ## full production build
	CGO_ENABLED=0 $(GO) build -ldflags="-buildid=" -trimpath

.PHONY: generate
generate: ## go generate ./...
	$(GO) generate ./...

.PHONY: test
test: ## run all tests
	$(GO) test ./...

# Cheap enough to run before every commit: concurrency is what breaks here.
.PHONY: test-race
test-race: ## run all tests with the race detector
	$(GO) test -race ./...

# -count=1: they are timing dependent, and a cached PASS means nothing.
.PHONY: soak
soak: ## run the long-running connection soak tests
	$(GO) test -tags soak -race -run TestSoak -count=1 -timeout 20m ./...

# No -race: it would make the instrument the bottleneck.
.PHONY: loadgen
loadgen: ## build the load generator (cmd/loadgen)
	$(GO) build -o loadgen ./cmd/loadgen

.PHONY: vendor
vendor: ## go mod vendor + tidy
	$(GO) mod vendor
	$(GO) mod tidy

# Not installed per run: a network round trip per commit gets the hook skipped.
.PHONY: check-tools
check-tools:
	@command -v golangci-lint >/dev/null || { echo "golangci-lint is not installed; run: make init"; exit 1; }

# No go vet as well: golangci-lint's govet runs cmd/vet's own analysers.
.PHONY: lint
lint: check-tools ## golangci-lint
	golangci-lint run

.PHONY: pre-commit
pre-commit: vendor lint test-race ## what the git pre-commit hook runs

.PHONY: init
init: tools hook ## set a clone up: install the linter and the git hook

.PHONY: hook
hook: ## install the pre-commit git hook
	printf '#!/bin/sh\nexec make -C "$$(git rev-parse --show-toplevel)" pre-commit\n' > .git/hooks/pre-commit
	chmod +x .git/hooks/pre-commit

# The /v2 matters: without it @latest quietly installs v1 over v2.
.PHONY: tools
tools: ## install the linter and govulncheck
	$(GO) install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest
	$(GO) install golang.org/x/vuln/cmd/govulncheck@latest

# Not in pre-commit: a hook that fails offline is a hook that gets skipped.
.PHONY: vulncheck
vulncheck: ## scan dependencies for known vulnerabilities
	govulncheck ./...

# The toolchain too: govulncheck reports stdlib CVEs against the go.mod pin.
.PHONY: update-deps
update-deps: tools ## update all go dependencies and the toolchain, vendor, vulncheck
	$(GO) get -u ./...
	$(GO) get toolchain@latest
	$(GO) mod tidy
	$(GO) mod vendor
	$(MAKE) test
	$(MAKE) vulncheck
