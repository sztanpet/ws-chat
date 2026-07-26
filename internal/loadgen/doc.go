// Package loadgen drives a ws-chat server the way a crowd does: a lot of
// connections, some fraction of which talk, and a report of what came back.
//
// It is a client and nothing else. It dials over the network like anybody
// else and measures what arrives on its own sockets, so the numbers it
// prints are the numbers a client would see. It shares the protocol
// constants with the server and none of its code: a benchmark that reaches
// into the thing it is measuring ends up measuring something else.
package loadgen
