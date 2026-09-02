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

- **`make update-deps` run 2026-07-26.** No module moved — everything was
  already at its latest version. What it did find, through govulncheck,
  was the toolchain: GO-2026-5856 (an ECH privacy leak in `crypto/tls`)
  is fixed in go1.26.5 and `go.mod` pinned go1.26.4. Bumped, and
  `update-deps` now runs `go get toolchain@latest` itself so the pin
  cannot be the thing nobody updates. Scan is clean.

- **Go 1.27, and the linters that were not ready for it** (`9da2e13`,
  `cd4fe43`, `f580f95`, `88f5557`, `dcc35d8`). `encoding/json/v2` shipped
  in 1.27 and is gated on the `go` directive rather than
  `GOEXPERIMENT=jsonv2`, so the module simply stopped compiling on a 1.27
  toolchain until go.mod moved. That deleted the Makefile export, the CI
  comment justifying it, and the README's build-constraint warning; the
  image moved to `golang:1.27`.

  The tooling was the real work. Three separate failures, none of which
  announces itself as a version problem:

  | tool | on 1.27 | done |
  |---|---|---|
  | golangci-lint | bundles honnef.co/go/tools v0.7.0, whose IR builder panics building `internal/poll`; the panic aborts the entire run | `.golangci.yml` disables `staticcheck` and `unused` |
  | staticcheck (2026.1) | cannot decode 1.27 export data, prints "internal error in importing" and **exits 0** | `tools` pins 2026.2rc1; `make lint` greps for that string and fails |
  | golangci-lint `@latest` | resolves against the v1 module path, downgrading an installed v2.12.2 to v1.64.8 | `tools` uses the `/v2` path |

  The exclusion preset added at the same time is not new policy -- it is
  what golangci-lint v1 excluded by default and v2 made opt-in, and its
  eight errcheck reports only became visible once a run got far enough to
  report anything.

  The silent-pass guard in `make lint` is the part worth keeping past this
  upgrade. A linter that exits 0 having checked nothing is worse than one
  that is missing, and `check-tools` cannot catch it: the binary is right
  there and runs.

- **The lint step broke in CI on the 1.27 bump, and the guard is why it
  was visible.** `/cache/gopath/bin` still held staticcheck 2026.1 from
  before, and the step only ran `make tools` when the binaries were
  *missing* -- so the stale one was never replaced and `make lint` failed
  on its own "checked nothing" check. Reproduced exactly in a
  `golang:1.27` container against a volume seeded with the old pair.

  The conditional turned out to be guarding nothing: with GOPATH and
  GOCACHE on `/cache`, a re-run of `make tools` **takes 1s** (58s only on
  a genuinely cold cache), because `go install` relinks what is already
  built. So it runs unconditionally now, in the vulncheck step too. Full
  pipeline -- test, lint, build -- verified green in-container starting
  from the stale cache.

  Two things changed while looking: **golangci-lint v2.13.0 fixed the
  honnef panic**, and staticcheck tagged **v0.8.0-rc.1 (2026.2rc1)**,
  which reads 1.27 export data. The pin moved from `@master` to that RC --
  a prerelease still beats a moving branch, since CI gets the same binary
  twice -- and comes off when 2026.2 ships. `staticcheck`/`unused` stay
  disabled in golangci-lint, but for a **different reason now**: not the
  panic, but that `make lint` already runs staticcheck standalone. The two
  copies disagree -- golangci-lint enables the QF* quickfix checks
  staticcheck itself leaves off, which is 5 reports here, none a bug and
  none worth taking.

- **Modernized against the current stdlib** (`dcc35d8`). gopls'
  `modernize` analyser, which now reports nothing: `WaitGroup.Go` for the
  private pump and seven concurrency tests, `errors.AsType[T]` for the
  declare-then-`errors.As` pair, plus `slices.Contains`, `reflect.TypeFor`
  and range-over-int. None of it is on the message path. The one
  non-mechanical change is `leaveAll`, which copied the membership map to
  clear the field -- every reader takes `membersMu`, so swapping the field
  under the lock already leaves the caller sole owner of the old map.

  **`httptest.NewTestServer` was tried and rejected.** It is 1.27's
  t.Cleanup-registering constructor, and it also returns an *unstarted*
  server backed by an *in-memory* network. The loadgen tests dial it as an
  ordinary network client -- which is the whole premise of the generator --
  so every dial failed with `unexpected url scheme: ""`. It suits a test
  driving a handler through `srv.Client()`, not one that needs a socket.

- **`make update-deps` run 2026-08-19** (`88f5557`). Only hujson moved
  (U+2028/U+2029 in a line comment is now an error rather than silently
  ending it). msgpack/v5 is at v5.4.1, the toolchain pin at go1.27.0, and
  **coder/websocket upstream is still v1.8.15** -- `SetWriteDeadline` has
  not landed in a release, so the fork replace in go.mod stays. govulncheck
  clean.

- **A second flake of the same family** (`TestUnbanSomebodyWhoIsGone`).
  `expectClosed` proves only that the *client* saw the close frame; the
  server tears the connection down afterwards on its own goroutine. Until
  that finishes the banned user is still in the directory and still in
  main, so the server-wide unban that follows found a target to announce
  and broadcast a MOD into the room — the admin's next `sync()` got that
  instead of its PONG. The same window produced "unexpected PART frame
  during connect" on the reconnect. Fixed the way `3c85675` fixed the
  first one: wait for the departing PART, which teardown broadcasts after
  leaving the directory, so it orders everything after it. Three of four
  concurrent 300-run loops failed before, 1800 runs clean after.

- **One flake fixed on the way** (`3c85675`). `make test` in that run hit
  "unexpected PART frame during connect" in
  `TestAccountLimitSurvivesReconnection`: a dropped connection's PART is
  broadcast after it leaves the directory, so an immediate reconnect can
  be subscribed in time to receive its own predecessor's departure. The
  test now keeps a second connection in the room and waits for the PART
  there before dialling again, which is what the moderation tests already
  do.

- **`make update-deps` run 2026-09-02.** Nothing in `require` moved:
  hujson and msgpack are at their latest, and coder/websocket upstream is
  **still v1.8.15**, so the `SetWriteDeadline` fork replace stays for a
  third run. The toolchain pin went go1.27.0 -> go1.27.1; the `go`
  directive stays at `1.27`, which is the language version and has no
  patch component to bump. `go fix ./...` -- the 1.27 tool, which is the
  modernizers, not the old API rewriter -- reported nothing, which is what
  `dcc35d8` having already run gopls `modernize` should look like.
  govulncheck clean.

  **The staticcheck pin came off, as its comment promised.** 2026.2 is
  released (v0.8.1), so `@latest` resolves to something that reads 1.27
  export data and `tools` no longer names a version. The grep in `make
  lint` stays: the pin was the fix for one release, the guard is the fix
  for the next toolchain that does this. golangci-lint is v2.13.2 and
  bundles honnef.co/go/tools v0.8.1 itself now, but `staticcheck` and
  `unused` stay off there for the reason already in `.golangci.yml` --
  running two copies of an analyser that disagree, not the old panic.

## Decisions

- **Every step shells out to the Makefile, never to `go` directly.** The
  Makefile is the only place that knows what flags a step needs, and a
  pipeline that spelled them out would be the copy that goes stale.
- **CI does not run `make vendor`.** It is the one `pre-commit` step left
  out: a CI run checks the tree it was handed rather than rewriting it. A
  stale `vendor/` fails the next step anyway.
- **Debian image (`golang:1.27`), not alpine.** `go test -race` needs cgo
  and a C toolchain; the race tests are the point of this suite.
- **Every Go cache lives on the agent's `/cache` volume**, the same
  convention as `kikapcsologo/backend`: `GOCACHE=/cache/go-build`,
  `GOPATH=/cache/gopath`, `GOLANGCI_LINT_CACHE=/cache/golangci-lint`.
  The homelab agent mounts `/cache` into every step (a docker named
  volume inside its dind), so it survives between runs, not just between
  steps. GOPATH carries the linters `make tools` installs, so the lint
  step builds them only when they are missing — over there that took
  lint from ~106s of a ~216s pipeline down to the cost of running it.
- **The linters are not installed by `make lint`.** A `go install` in
  front of every commit is a network round trip between somebody and
  their commit, and the response to that is to stop running the hook.
- **govulncheck is cron-only**, for the same reason it is out of the
  pre-commit hook: it needs the vuln db over the network, so it fails for
  reasons unrelated to the commit.

## Pending / known rough edges

- **The first run after `/cache` is cleared is cold**: every vendored
  dependency recompiles and the linters are built from source, minutes of
  it. Nothing to do about it beyond not clearing the cache.
- **The linters lag every Go release, and `make tools` cannot fix that by
  itself.** Both have caught up with 1.27 and no version is pinned any
  more, but the failure mode is not: a linter that cannot read the
  toolchain's export data reports success. That is what the grep in `make
  lint` is for, and it is the thing to reach for on the next bump. See
  the Go 1.27 entry under Done.
- **No cron exists yet.** The vulncheck step needs one adding in the
  repository's Woodpecker settings or it never runs.
- **Nothing checks that `vendor/` is in sync** beyond the build failing.
