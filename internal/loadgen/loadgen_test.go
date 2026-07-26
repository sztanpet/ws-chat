package loadgen

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/sztanpet/ws-chat/internal/proto"
)

// The stub below is deliberately not the real server: what is under test
// here is the load generator's own arithmetic — who speaks, where, and what
// it should therefore have received. Running it against the real server
// would test the server, which the server's own tests already do, and would
// make these results depend on its timing.

// stub is the smallest thing that behaves like a chat server: READY, an
// autojoin, per-connection membership, and a MSG fanned out to whoever is
// in the channel it names.
type stub struct {
	*httptest.Server

	mu      sync.Mutex
	members map[*stubConn]map[string]bool
	joins   int
	parts   int
	pings   int
	seq     int
}

type stubConn struct {
	ws    *websocket.Conn
	codec proto.Codec
	nick  string
	mu    sync.Mutex // one writer at a time, unlike the real server
}

const stubDefault = "main"

func newStub(t *testing.T) *stub {
	t.Helper()
	s := &stub{members: make(map[*stubConn]map[string]bool)}
	s.Server = httptest.NewServer(http.HandlerFunc(s.serve))
	t.Cleanup(s.Close)
	return s
}

func (s *stub) url() string {
	return "ws" + strings.TrimPrefix(s.Server.URL, "http") + "/ws"
}

func (s *stub) serve(w http.ResponseWriter, r *http.Request) {
	ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		Subprotocols: proto.Subprotocols(),
	})
	if err != nil {
		return
	}
	defer func() { _ = ws.CloseNow() }()

	codec, err := proto.ByName(ws.Subprotocol())
	if err != nil {
		return
	}

	s.mu.Lock()
	s.seq++
	c := &stubConn{ws: ws, codec: codec, nick: fmt.Sprintf("user%d", s.seq)}
	s.members[c] = map[string]bool{stubDefault: true}
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.members, c)
		s.mu.Unlock()
	}()

	ctx := r.Context()
	s.write(ctx, c, proto.NewReady(c.nick))
	s.write(ctx, c, proto.NewJoin(proto.Join{Channel: stubDefault, Nick: c.nick}))

	for {
		typ, data, err := ws.Read(ctx)
		if err != nil {
			return
		}
		if (typ == websocket.MessageBinary) != codec.Binary() {
			return
		}

		var cmd proto.Command
		if err := codec.Decode(data, &cmd); err != nil {
			return
		}
		s.handle(ctx, c, cmd)
	}
}

func (s *stub) handle(ctx context.Context, c *stubConn, cmd proto.Command) {
	switch cmd.Verb {
	case proto.VerbPing:
		s.mu.Lock()
		s.pings++
		s.mu.Unlock()
		s.write(ctx, c, proto.NewPong())

	case proto.VerbJoin:
		s.mu.Lock()
		s.joins++
		s.members[c][cmd.Channel] = true
		s.mu.Unlock()
		s.write(ctx, c, proto.NewJoin(proto.Join{Channel: cmd.Channel, Nick: c.nick}))

	case proto.VerbPart:
		s.mu.Lock()
		s.parts++
		delete(s.members[c], cmd.Channel)
		s.mu.Unlock()
		s.write(ctx, c, proto.NewPart(cmd.Channel, c.nick))

	case proto.VerbMsg:
		channel := cmd.Channel
		if channel == "" {
			channel = stubDefault
		}
		s.broadcast(ctx, channel, c.nick, cmd.Data)
	}
}

func (s *stub) broadcast(ctx context.Context, channel, nick, data string) {
	s.mu.Lock()
	var to []*stubConn
	for c, in := range s.members {
		if in[channel] {
			to = append(to, c)
		}
	}
	s.mu.Unlock()

	msg := proto.NewMsg(proto.Msg{Channel: channel, ID: 1, Nick: nick, Data: data,
		Timestamp: time.Now().UnixMilli()})
	for _, c := range to {
		s.write(ctx, c, msg)
	}
}

func (s *stub) write(ctx context.Context, c *stubConn, payload proto.Outbound) {
	frame, err := c.codec.Encode(payload)
	if err != nil {
		return
	}
	typ := websocket.MessageText
	if c.codec.Binary() {
		typ = websocket.MessageBinary
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_ = c.ws.Write(ctx, typ, frame)
}

func (s *stub) counts() (joins, parts, pings int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.joins, s.parts, s.pings
}

func baseConfig(s *stub) Config {
	return Config{
		URL:          s.url(),
		Conns:        6,
		SpeakPercent: 50,
		Rate:         50,
		MessageSize:  64,
		Channels:     1,
		Duration:     300 * time.Millisecond,
		PingEvery:    time.Second,
	}
}

func mustRun(t *testing.T, cfg Config) *Result {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	r, err := Run(ctx, cfg)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	return r
}

// The whole thing end to end: everybody connects, half of them talk, and
// every message comes back to every member of the room.
func TestRun(t *testing.T) {
	s := newStub(t)
	r := mustRun(t, baseConfig(s))

	if r.Dialed != 6 || r.Failed != 0 {
		t.Fatalf("dialled %d, %d failed; want 6 and 0 (%v)", r.Dialed, r.Failed, r.DialErrs)
	}
	if r.Lost != 0 {
		t.Errorf("lost %d connections: %v", r.Lost, r.Losses)
	}
	if len(r.Refusals) != 0 {
		t.Errorf("the server refused something: %v", r.Refusals)
	}
	if r.Garbage != 0 {
		t.Errorf("%d frames would not decode", r.Garbage)
	}

	// Three speakers at 50/s for 300ms. Timing is timing, so this only
	// insists that they all spoke and that nobody sent for everybody.
	if r.Sent < 3 || r.Sent > 3*50 {
		t.Errorf("sent %d messages, want between 3 and 150", r.Sent)
	}
	if r.Expected != r.Sent*6 {
		t.Errorf("expected %d deliveries for %d messages in a room of 6, want %d",
			r.Expected, r.Sent, r.Sent*6)
	}

	// Received can lag Expected by whatever was in flight when the clock
	// stopped, but not by much.
	if r.Received < r.Expected*8/10 {
		t.Errorf("received %d of %d expected", r.Received, r.Expected)
	}
	if r.Latency.Count() != r.Received {
		t.Errorf("timed %d of %d received messages", r.Latency.Count(), r.Received)
	}
	if r.Latency.Max() > 5*time.Second {
		t.Errorf("max latency %v against a stub on loopback", r.Latency.Max())
	}
}

// A listener that never speaks still has to ping, or the real server closes
// it for being idle.
func TestListenersPing(t *testing.T) {
	s := newStub(t)
	cfg := baseConfig(s)
	cfg.SpeakPercent = 0
	cfg.PingEvery = 20 * time.Millisecond
	cfg.Duration = 200 * time.Millisecond

	r := mustRun(t, cfg)
	if r.Sent != 0 {
		t.Errorf("sent %d messages with nobody speaking", r.Sent)
	}
	if _, _, pings := s.counts(); pings < 6 {
		t.Errorf("%d pings from 6 idle connections over 10 ping intervals", pings)
	}
}

// Spread over several channels, a connection joins its own and leaves the
// one it was put in, and a message reaches that room only.
func TestRunSpreadsOverChannels(t *testing.T) {
	s := newStub(t)
	cfg := baseConfig(s)
	cfg.Conns = 6
	cfg.Channels = 3
	cfg.SpeakPercent = 100

	r := mustRun(t, cfg)

	if joins, parts, _ := s.counts(); joins != 6 || parts != 6 {
		t.Errorf("%d joins and %d parts from 6 connections, want 6 of each", joins, parts)
	}
	if r.Expected != r.Sent*2 {
		t.Errorf("expected %d deliveries for %d messages in rooms of 2, want %d",
			r.Expected, r.Sent, r.Sent*2)
	}
	if r.Received < r.Expected*8/10 {
		t.Errorf("received %d of %d expected", r.Received, r.Expected)
	}
	// Six speakers in three rooms of two: anybody hearing more than their
	// own room means the spread did not happen.
	if r.Received > r.Sent*2 {
		t.Errorf("received %d for %d messages: traffic crossed channels", r.Received, r.Sent)
	}
}

func TestRunOverEveryCodec(t *testing.T) {
	for _, name := range []string{proto.NameJSON, proto.NameMsgPack} {
		t.Run(name, func(t *testing.T) {
			s := newStub(t)
			cfg := baseConfig(s)
			cfg.Codec = name

			r := mustRun(t, cfg)
			if r.Dialed != 6 || r.Sent == 0 || r.Received == 0 || r.Garbage != 0 {
				t.Fatalf("codec %s: %+v", name, r)
			}
		})
	}
}

// A server that is not there has to produce a report rather than an error:
// counted dial failures are a result, and half a run reported is worth more
// than none.
func TestRunReportsDialFailures(t *testing.T) {
	s := newStub(t)
	cfg := baseConfig(s)
	cfg.URL = s.url()
	s.Close() // nothing is listening now

	r := mustRun(t, cfg)
	if r.Dialed != 0 || r.Failed != 6 {
		t.Fatalf("dialled %d, %d failed; want 0 and 6", r.Dialed, r.Failed)
	}
	if len(r.DialErrs) == 0 {
		t.Error("no dial error was recorded")
	}
	if r.Elapsed <= 0 {
		t.Error("the run did not report an elapsed time")
	}
}

// The context is the only way to stop an open-ended run, so it had better
// work.
func TestRunStopsOnContext(t *testing.T) {
	s := newStub(t)
	cfg := baseConfig(s)
	cfg.Duration = 0 // until cancelled

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	r, err := Run(ctx, cfg)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if took := time.Since(start); took > 10*time.Second {
		t.Fatalf("the run took %v to notice a cancelled context", took)
	}
	if r.Sent == 0 {
		t.Error("nothing was sent before the run was cancelled")
	}
}

func TestRunRejectsNonsense(t *testing.T) {
	tests := []struct {
		name  string
		tweak func(*Config)
	}{
		{"no url", func(c *Config) { c.URL = "" }},
		{"no connections", func(c *Config) { c.Conns = 0 }},
		{"too many speakers", func(c *Config) { c.SpeakPercent = 101 }},
		{"no rate", func(c *Config) { c.Rate = 0 }},
		{"no channels", func(c *Config) { c.Channels = 0 }},
		{"empty messages", func(c *Config) { c.MessageSize = 0 }},
		{"no pings", func(c *Config) { c.PingEvery = 0 }},
		{"unknown codec", func(c *Config) { c.Codec = "chat.xml" }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{URL: "ws://127.0.0.1:1/ws", Conns: 1, SpeakPercent: 100,
				Rate: 1, MessageSize: 8, Channels: 1, PingEvery: time.Second}
			tt.tweak(&cfg)
			if _, err := Run(context.Background(), cfg); err == nil {
				t.Fatal("accepted it")
			}
		})
	}
}

// Who speaks is arithmetic, and it is worth checking on its own: the
// percentage has to come out right and the speakers have to be spread
// through the range, or a run over several channels puts all of them in the
// first room.
func TestSpeakerSpread(t *testing.T) {
	tests := []struct {
		conns   int
		percent float64
		want    int
	}{
		{100, 10, 10},
		{100, 0, 0},
		{100, 100, 100},
		{7, 50, 3},
		{1, 100, 1},
		{1000, 0.5, 5},
	}

	for _, tt := range tests {
		cfg := Config{Conns: tt.conns, SpeakPercent: tt.percent, Channels: 4}
		got := 0
		for i := range tt.conns {
			if cfg.speaks(i) {
				got++
			}
		}
		if got != tt.want {
			t.Errorf("%d conns at %g%%: %d speakers, want %d", tt.conns, tt.percent, got, tt.want)
		}
		if got != cfg.speakers() {
			t.Errorf("%d conns at %g%%: speaks() picked %d, speakers() says %d",
				tt.conns, tt.percent, got, cfg.speakers())
		}

		// Every channel with members should get a speaker, once there are
		// more speakers than channels.
		if got >= cfg.Channels {
			for ch := range cfg.Channels {
				found := false
				for i := ch; i < tt.conns; i += cfg.Channels {
					found = found || cfg.speaks(i)
				}
				if !found {
					t.Errorf("%d conns at %g%%: channel %d has no speaker", tt.conns, tt.percent, ch)
				}
			}
		}
	}
}

func TestMembersInAddUp(t *testing.T) {
	for conns := 1; conns <= 20; conns++ {
		for channels := 1; channels <= 5; channels++ {
			cfg := Config{Conns: conns, Channels: channels}

			total := 0
			for i := range channels {
				total += cfg.membersIn(i)
			}
			if total != conns {
				t.Fatalf("%d conns over %d channels adds up to %d", conns, channels, total)
			}

			// And it has to agree with what channelName hands out.
			counted := make([]int, channels)
			for i := range conns {
				counted[i%channels]++
			}
			for i, n := range counted {
				if cfg.membersIn(i) != n {
					t.Fatalf("%d conns over %d channels: membersIn(%d) = %d, really %d",
						conns, channels, i, cfg.membersIn(i), n)
				}
			}
		}
	}
}

// Losses are counted in a map keyed by the reason, so the reason has to
// come from a closed set. A net.OpError carries the peer address, which
// turned three thousand dropped sockets into three thousand report lines
// the first time this ran at scale.
func TestReasonsAreABoundedSet(t *testing.T) {
	addr := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 51234}
	peer := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 8099}

	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			"lag drop",
			websocket.CloseError{Code: websocket.StatusPolicyViolation, Reason: "too slow"},
			"StatusPolicyViolation: too slow",
		},
		{
			"close with no reason",
			websocket.CloseError{Code: websocket.StatusGoingAway},
			"StatusGoingAway",
		},
		{
			"reset, wrapped the way the library wraps it",
			fmt.Errorf("failed to get reader: failed to read frame header: %w",
				&net.OpError{Op: "read", Net: "tcp", Source: addr, Addr: peer,
					Err: os.NewSyscallError("read", syscall.ECONNRESET)}),
			"read: connection reset by peer",
		},
		{
			"our own close racing the read",
			fmt.Errorf("failed to read: %w",
				&net.OpError{Op: "read", Net: "tcp", Source: addr, Addr: peer, Err: net.ErrClosed}),
			"connection closed",
		},
		{"truncated frame", fmt.Errorf("failed to read payload: %w", io.ErrUnexpectedEOF), "eof"},
		{"deadline", fmt.Errorf("write: %w", context.DeadlineExceeded), "timed out"},
		{"cancelled", context.Canceled, "cancelled"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := reasonOf(tt.err)
			if got != tt.want {
				t.Errorf("reasonOf(%v) = %q, want %q", tt.err, got, tt.want)
			}
			if strings.Contains(got, "51234") || strings.Contains(got, "8099") {
				t.Errorf("reason %q names an address, so every socket gets its own line", got)
			}
		})
	}
}

func TestPayloadIsSizedAndTimed(t *testing.T) {
	for _, size := range []int{1, 8, 30, 64, 512} {
		c := &client{cfg: Config{MessageSize: size}, pad: strings.Repeat("x", size)}
		body := c.payload()

		if size >= 20 && len(body) != size {
			t.Errorf("size %d: payload is %d bytes", size, len(body))
		}
		if len(body) > max(size, 20) {
			t.Errorf("size %d: payload is %d bytes, longer than the timestamp needs", size, len(body))
		}

		d, ok := latencyOf(body)
		if !ok {
			t.Fatalf("size %d: no timestamp in %q", size, body)
		}
		if d < 0 || d > time.Minute {
			t.Errorf("size %d: latency came out as %v", size, d)
		}
	}

	if _, ok := latencyOf("a message somebody actually typed"); ok {
		t.Error("a message with no timestamp was timed anyway")
	}
	if _, ok := latencyOf("|nothing in front"); ok {
		t.Error("an empty timestamp was accepted")
	}
}
