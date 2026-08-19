module github.com/sztanpet/ws-chat

go 1.27

toolchain go1.27.0

require (
	github.com/coder/websocket v1.8.15
	github.com/tailscale/hujson v0.0.0-20260722022634-78b5b162ee49
	github.com/vmihailenco/msgpack/v5 v5.4.1
)

require github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect

// TODO: REVERT THIS once SetWriteDeadline is upstream.
//
// The server calls Conn.SetWriteDeadline, which does not exist in any
// released coder/websocket. It is the feat/conn-set-write-deadline branch of
// the fork (github.com/sztanpet/websocket, commit 9f84ce4), submitted as the
// PR written up in code-websocket-pr/README.md.
//
// To revert, once it lands in a release: drop this directive, bump the
// require above to that release, run `make vendor`, and put the two
// deadline-shaped edits in conn.go back onto a context — the diff to undo is
// code-websocket-pr/ws-chat-caller.patch.
//
// A directory replacement rather than github.com/sztanpet/websocket@version,
// because the fork's go.mod still declares the upstream module path, which a
// renamed replacement path would be rejected for. Builds do not depend on
// that directory: the code is vendored, so CI and a fresh clone compile from
// vendor/ and only `make vendor` needs the fork checked out beside this repo.
replace github.com/coder/websocket => ../coder-websocket
