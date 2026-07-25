# All tooling lives here. `make help` lists targets.

GO := go

# make installed go tools (staticcheck, golangci-lint) visible even
# when GOPATH/bin is not on the user's PATH
export PATH := $(shell $(GO) env GOPATH)/bin:$(PATH)

.PHONY: help
help: ## list available targets
	@grep -hE '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "%-13s %s\n", $$1, $$2}'

.PHONY: run
run: ## build with -race and run the server
	$(GO) build -race && ./ws-chat

.PHONY: build
build: generate ## full production build
	$(GO) build -ldflags="-buildid=" -trimpath -race

.PHONY: generate
generate: ## go generate ./...
	$(GO) generate ./...

.PHONY: test
test: ## run all tests
	$(GO) test ./...

# The websocket fan-out is the part that breaks under concurrency, not
# under load in the profiling sense. Keep a -race run cheap enough that
# it is the default before every commit.
.PHONY: test-race
test-race: ## run all tests with the race detector
	$(GO) test -race ./...

# Soak tests spin up thousands of live connections and take minutes, so
# they are gated behind the `soak` build tag and stay out of `make test`
# and the pre-commit hook. -count=1 disables caching: they are timing
# dependent and a cached PASS means nothing.
.PHONY: soak
soak: ## run the long-running connection soak tests
	$(GO) test -tags soak -race -run TestSoak -count=1 -timeout 20m ./...

.PHONY: vendor
vendor: ## go mod vendor + tidy
	$(GO) mod vendor
	$(GO) mod tidy

.PHONY: lint
lint: ## go vet, staticcheck, golangci-lint
	$(GO) vet ./...
	staticcheck ./...
	golangci-lint run

.PHONY: pre-commit
pre-commit: vendor lint test-race ## what the git pre-commit hook runs

.PHONY: hook
hook: ## install the pre-commit git hook
	printf '#!/bin/sh\nexec make -C "$$(git rev-parse --show-toplevel)" pre-commit\n' > .git/hooks/pre-commit
	chmod +x .git/hooks/pre-commit

.PHONY: tools
tools: ## install the linters and govulncheck
	$(GO) install honnef.co/go/tools/cmd/staticcheck@latest
	$(GO) install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	$(GO) install golang.org/x/vuln/cmd/govulncheck@latest

# not part of pre-commit on purpose: govulncheck talks to the vuln db
# over the network, and a hook that fails offline is a hook that gets
# skipped. It runs whenever the dependency set actually changes.
.PHONY: vulncheck
vulncheck: ## scan dependencies for known vulnerabilities
	govulncheck ./...

.PHONY: update-deps
update-deps: tools ## update all go dependencies, vendor, vulncheck
	$(GO) get -u ./...
	$(GO) mod tidy
	$(GO) mod vendor
	$(MAKE) test
	$(MAKE) vulncheck
