# CI and developer tooling

Working state for `.woodpecker.yaml` and the Makefile targets around it.

## Done

- **`.woodpecker.yaml`** (`5e79713`). Woodpecker 3 syntax, validated with
  `docker run --rm -v "$PWD:/repo" -w /repo woodpeckerci/woodpecker-cli:v3
  lint .woodpecker.yaml`. Four steps, sequential:

  | step | runs | when |
  |---|---|---|
  | test | `make test-race` | push, PR, manual, cron |
  | lint | `make tools` + `make lint` | push, PR, manual, cron |
  | build | `make build` + `make loadgen` | push, PR, manual, cron |
  | vulncheck | `make tools` + `make vulncheck` | cron only |

- **`make init`** (`29adc29`) — `tools` + `hook`, the one thing a fresh
  clone runs. `make lint` gained a `check-tools` prerequisite that fails
  with "run: make init" rather than dying halfway through on a missing
  binary. The hook itself is unchanged: `.git/hooks/pre-commit` execs
  `make pre-commit` (vendor + lint + `-race` tests), installed and
  exercised on the commit that added it.

- **`make lint` made green** (`1e950cf`). It had never been run on this
  tree: 19 findings, none a bug. ST1005 on validation errors that start
  with a config field name (now "field Addr must not be empty"), errcheck
  on `CloseNow()` (now `_ =`, the socket is going either way), and SA4000
  on `!b.AllowAt(later) || !b.AllowAt(later)` in the ratelimit test, which
  was correct but reads like a copy-paste bug and is now two variables.

## Decisions

- **Every step shells out to the Makefile, never to `go` directly.** The
  Makefile is the only place that knows about `GOEXPERIMENT=jsonv2`, and a
  pipeline that spelled the flags out would be the copy that goes stale.
- **CI does not run `make vendor`.** It is the one `pre-commit` step left
  out: a CI run checks the tree it was handed rather than rewriting it. A
  stale `vendor/` fails the next step anyway.
- **Debian image (`golang:1.26`), not alpine.** `go test -race` needs cgo
  and a C toolchain; the race tests are the point of this suite.
- **`workspace.base: /go`** so the shared volume lands on GOPATH: the
  module cache and the linters `make tools` builds are shared between
  steps, so they are built once per pipeline rather than once per step.
- **The linters are not installed by `make lint`.** A `go install` in
  front of every commit is a network round trip between somebody and
  their commit, and the response to that is to stop running the hook.
- **govulncheck is cron-only**, for the same reason it is out of the
  pre-commit hook: it needs the vuln db over the network, so it fails for
  reasons unrelated to the commit.

## Pending / known rough edges

- **The lint step builds golangci-lint from source on every pipeline**
  (minutes cold). Woodpecker's volume is per-run; making it survive needs
  a host volume, which needs the repository marked trusted in Woodpecker.
- **`make tools` installs golangci-lint from the v1 module path**, which
  `@latest` now pins to v1.64.8 forever (v2 lives at
  `github.com/golangci/golangci-lint/v2/cmd/golangci-lint`). v1.64.8 still
  builds and runs clean under Go 1.26; moving to v2 means a `.golangci.yml`
  in the v2 format and re-triaging its default linter set.
- **No cron exists yet.** The vulncheck step needs one adding in the
  repository's Woodpecker settings or it never runs.
- **Nothing checks that `vendor/` is in sync** beyond the build failing.
