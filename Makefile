# All tooling lives here. `make help` lists targets.

GO := go

# make installed go tools (staticcheck, golangci-lint) visible even
# when GOPATH/bin is not on the user's PATH
export PATH := $(shell $(GO) env GOPATH)/bin:$(PATH)

# encoding/json/v2 only exists when the toolchain is built with its
# experiment enabled, and the JSON wire codec uses it, so NOTHING in this
# module compiles without this — including go vet, staticcheck and
# golangci-lint, which are compilers with opinions. Anything run by hand
# needs it too:
#
#	GOEXPERIMENT=jsonv2 go test ./...
#
# Appended to whatever is already set rather than replacing it, since the
# variable is a list and somebody may have their own reasons.
comma := ,
export GOEXPERIMENT := jsonv2$(if $(GOEXPERIMENT),$(comma)$(GOEXPERIMENT))

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

# No -race here, deliberately. The load generator is the measuring
# instrument: instrumenting it makes it ten times slower and it becomes the
# bottleneck instead of the server.
.PHONY: loadgen
loadgen: ## build the load generator (cmd/loadgen)
	$(GO) build -o loadgen ./cmd/loadgen

.PHONY: vendor
vendor: ## go mod vendor + tidy
	$(GO) mod vendor
	$(GO) mod tidy

# The linters are installed by `make init`, not by every lint run: a
# `go install` in front of every commit is a network round trip between
# somebody and their commit, and the first thing they would do about it
# is stop running the hook.
.PHONY: check-tools
check-tools:
	@for t in staticcheck golangci-lint; do \
		command -v $$t >/dev/null || { echo "$$t is not installed; run: make init"; exit 1; }; \
	done

.PHONY: lint
lint: check-tools ## go vet, staticcheck, golangci-lint
	$(GO) vet ./...
	staticcheck ./...
	golangci-lint run

.PHONY: pre-commit
pre-commit: vendor lint test-race ## what the git pre-commit hook runs

# One command for a fresh clone: the linters the hook needs, and the
# hook itself. Everything else in here assumes it has been run.
.PHONY: init
init: tools hook ## set a clone up: install the linters and the git hook

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

# The toolchain is updated along with everything else, because it is a
# dependency like any other: govulncheck reports standard library CVEs
# against the version pinned in go.mod, and Go's patch releases are
# mostly security fixes. Bumping it here is safe because the two steps
# that follow are the whole test suite and a vulnerability scan.
.PHONY: update-deps
update-deps: tools ## update all go dependencies and the toolchain, vendor, vulncheck
	$(GO) get -u ./...
	$(GO) get toolchain@latest
	$(GO) mod tidy
	$(GO) mod vendor
	$(MAKE) test
	$(MAKE) vulncheck
