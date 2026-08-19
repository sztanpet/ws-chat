package loadgen

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/sztanpet/ws-chat/internal/proto"
)

// The generator labels its goroutines for the same reason the server does,
// and for one of its own: it shares a machine with the thing it is
// measuring, and "some of the latency is the measuring" is a claim a
// profile of this process should be able to settle. A run is a handful of
// roles — dialing, reading, speaking, printing — and which of them is
// burning the CPU is the question worth asking.
//
// Same convention as the server: one key, values from a closed set.
const labelTask = "task"

const (
	taskBarrier  = "barrier"
	taskDialer   = "dialer"
	taskClient   = "client"
	taskReader   = "reader"
	taskProgress = "progress"
)

// Config is a run.
type Config struct {
	// URL is the WebSocket endpoint, ws:// or wss://.
	URL string

	// Conns is how many connections to hold open.
	Conns int

	// SpeakPercent is what fraction of them talk. The rest connect, join and
	// listen, which is the majority in any real room and the load that
	// actually hurts: a message costs one send and N deliveries.
	SpeakPercent float64

	// Rate is messages per second per speaking connection.
	Rate float64

	// MessageSize is the body length in bytes, including the timestamp the
	// generator puts at the front. The server refuses anything over its
	// MaxMessage, which is 512 by default.
	MessageSize int

	// Channels is how many rooms the connections are spread over, round
	// robin. One means everybody stays where the server put them.
	Channels int

	// Duration is how long to talk for, measured from the moment every
	// connection is in place rather than from the first dial. Zero runs
	// until the context is cancelled.
	Duration time.Duration

	// Ramp spreads the dialling out. Ten thousand simultaneous handshakes
	// measure the listen backlog, not the chat server.
	Ramp time.Duration

	// PingEvery keeps otherwise silent connections alive: the server closes
	// anything that has sent nothing at all for IdleTimeout, and a listener
	// is exactly that.
	PingEvery time.Duration

	// Codec is the subprotocol to negotiate. Empty is JSON.
	Codec string

	// Progress is how often to write a progress line to Out. Zero disables
	// it; a nil Out is silent whatever this says.
	Progress time.Duration
	Out      io.Writer
}

// writeTimeout bounds one socket write. A load generator that blocks
// forever on a stalled server reports nothing at all.
const writeTimeout = 10 * time.Second

// readLimit is generous: a BACKLOG frame carries the whole replay window in
// one message, and being disconnected for reading a big one would look like
// a server fault in the report.
const readLimit = 1 << 20

func (c Config) check() error {
	switch {
	case c.URL == "":
		return errors.New("field URL must not be empty")
	case c.Conns < 1:
		return errors.New("field Conns must be at least 1")
	case c.SpeakPercent < 0 || c.SpeakPercent > 100:
		return errors.New("field SpeakPercent must be between 0 and 100")
	case c.Channels < 1:
		return errors.New("field Channels must be at least 1")
	case c.MessageSize < 1:
		return errors.New("field MessageSize must be at least 1")
	case c.PingEvery <= 0:
		return errors.New("field PingEvery must be positive")
	case c.speakers() > 0 && c.Rate <= 0:
		return errors.New("field Rate must be positive when anybody is speaking")
	}
	return nil
}

// speakers is how many connections talk.
func (c Config) speakers() int { return int(float64(c.Conns) * c.SpeakPercent / 100) }

// speakersIn is how many of them talk in channel i. The remainder is spread
// one per channel rather than dumped on the first, so the rooms differ by at
// most one speaker.
func (c Config) speakersIn(i int) int {
	return (c.speakers() - i + c.Channels - 1) / c.Channels
}

// speaks reports whether connection i is one of the speakers.
//
// The choice is made within the connection's own channel, not across the
// whole run, because the obvious version — every Nth connection speaks —
// aliases against round-robin channel assignment and leaves whole rooms
// silent. Ten percent of a hundred connections over four channels is every
// tenth one, which is only ever channels 1 and 3.
func (c Config) speaks(i int) bool {
	ch := i % c.Channels
	pos := i / c.Channels // where it sits in its own channel
	members, speakers := c.membersIn(ch), c.speakersIn(ch)
	return pos*speakers/members != (pos+1)*speakers/members
}

// membersIn is how many connections are in channel i.
func (c Config) membersIn(i int) int {
	return (c.Conns - i + c.Channels - 1) / c.Channels
}

// channelName is what connection i talks in. Empty means whatever the server
// autojoined it into, which is the single-channel case: there is no reason
// to name a room when everybody is in the same one.
func (c Config) channelName(i int) string {
	if c.Channels == 1 {
		return ""
	}
	return fmt.Sprintf("bench%d", i%c.Channels)
}

// Run holds Conns connections open and reports what happened.
//
// It returns when the duration is up or ctx is cancelled, and it does not
// abort because connections failed: a load test that gives up when three
// dials out of ten thousand are refused is a load test that never finishes.
// The failures are counted and printed instead.
func Run(ctx context.Context, cfg Config) (*Result, error) {
	if err := cfg.check(); err != nil {
		return nil, err
	}
	codec, err := codecByName(cfg.Codec)
	if err != nil {
		return nil, err
	}

	st := newStats(cfg.Channels)
	pad := strings.Repeat("x", cfg.MessageSize)

	runCtx, stop := context.WithCancel(ctx)
	defer stop()

	// Nobody speaks until everybody is connected. Otherwise the first
	// speaker is fanning out to a room that is still filling up, and the
	// delivery ratio is a measure of the ramp rather than of the server.
	var settling sync.WaitGroup
	settling.Add(cfg.Conns)
	start := make(chan struct{})
	go pprof.Do(runCtx, pprof.Labels(labelTask, taskBarrier), func(context.Context) {
		settling.Wait()
		close(start)
	})

	var wg sync.WaitGroup
	wg.Add(cfg.Conns)
	// The labelled context shadows runCtx deliberately: it is the same
	// cancellation with the labels attached, and every connection dialed
	// below has to inherit them.
	go pprof.Do(runCtx, pprof.Labels(labelTask, taskDialer), func(runCtx context.Context) {
		pause := time.Duration(0)
		if cfg.Ramp > 0 {
			pause = cfg.Ramp / time.Duration(cfg.Conns)
		}
		for i := range cfg.Conns {
			if pause > 0 {
				select {
				case <-time.After(pause):
				case <-runCtx.Done():
				}
			}
			c := &client{
				cfg: cfg, stats: st, codec: codec, pad: pad,
				index: i, speaks: cfg.speaks(i), channel: cfg.channelName(i),
				joins: make(chan string, 4),
			}
			go func() {
				defer wg.Done()
				pprof.Do(runCtx, pprof.Labels(labelTask, taskClient), func(ctx context.Context) {
					c.run(ctx, &settling, start)
				})
			}()
		}
	})

	select {
	case <-start:
	case <-runCtx.Done():
	}

	begin := time.Now()
	if cfg.Progress > 0 && cfg.Out != nil {
		go pprof.Do(runCtx, pprof.Labels(labelTask, taskProgress), func(ctx context.Context) {
			progress(ctx, cfg, st, begin)
		})
	}

	if cfg.Duration > 0 {
		timer := time.NewTimer(cfg.Duration)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-runCtx.Done():
		}
	} else {
		<-runCtx.Done()
	}
	elapsed := time.Since(begin)

	stop()
	wg.Wait()
	return st.snapshot(cfg, elapsed), nil
}

// progress writes one line per interval, so a long run shows whether it is
// still healthy rather than only saying so at the end.
func progress(ctx context.Context, cfg Config, st *stats, begin time.Time) {
	t := time.NewTicker(cfg.Progress)
	defer t.Stop()

	var lastSent, lastRecv uint64
	last := begin
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			secs := now.Sub(last).Seconds()
			sent, recv := st.sent.Load(), st.received.Load()
			fmt.Fprintf(cfg.Out, "[%6s] up %d  sent %d (%s/s)  recv %d (%s/s)  lost %d\n",
				now.Sub(begin).Round(time.Second),
				st.dialed.Load()-st.lost.Load(),
				sent, rate(sent-lastSent, secs),
				recv, rate(recv-lastRecv, secs),
				st.lost.Load())
			lastSent, lastRecv, last = sent, recv, now
		}
	}
}

// client is one connection.
type client struct {
	cfg   Config
	stats *stats
	codec codec
	pad   string

	index   int
	speaks  bool
	channel string // where it talks; empty until settle learns the default

	ws   *websocket.Conn
	nick string
	lat  Histogram

	// joins carries this connection's own JOIN frames from the reader to
	// settle. Everybody else's are noise — a room of ten thousand announces
	// ten thousand joins to each of them.
	joins chan string
}

func (c *client) run(ctx context.Context, settling *sync.WaitGroup, start <-chan struct{}) {
	// Whatever happens, the barrier has to come down exactly once: a
	// connection that never dials must not hold the run hostage.
	settled := sync.OnceFunc(settling.Done)
	defer settled()

	if err := c.dial(ctx); err != nil {
		c.stats.dialErr(reasonOf(err))
		return
	}
	defer func() { _ = c.ws.CloseNow() }()
	c.stats.dialed.Add(1)

	// The reader runs for the whole life of the connection and owns every
	// measurement. When it stops, this connection is over, which is what
	// connCtx tells the sender.
	connCtx, dead := context.WithCancel(ctx)

	done := make(chan struct{})
	go func() {
		defer close(done)
		defer dead()
		pprof.Do(connCtx, pprof.Labels(labelTask, taskReader), c.read)
	}()

	// Registered in this order so they run in the other one: cancel the
	// reader's context, wait for it to stop, and only then touch the
	// histogram it owns.
	defer func() {
		<-done
		c.stats.mergeLatency(&c.lat)
	}()
	defer dead()

	if !c.settle(connCtx) {
		return
	}
	settled()

	select {
	case <-start:
	case <-connCtx.Done():
		return
	}

	c.speak(connCtx)
	c.ws.Close(websocket.StatusNormalClosure, "")
}

func (c *client) dial(ctx context.Context) error {
	ws, _, err := websocket.Dial(ctx, c.cfg.URL, &websocket.DialOptions{
		Subprotocols: []string{c.codec.name},
	})
	if err != nil {
		return err
	}
	ws.SetReadLimit(readLimit)
	c.ws = ws

	// READY first, always, and it carries the nick — which the reader needs
	// before it starts, to tell this connection's own JOIN from the ten
	// thousand others it is about to be told about.
	var f frame
	if err := c.readFrame(ctx, &f); err != nil {
		_ = ws.CloseNow()
		return err
	}
	if f.Verb != proto.VerbReady {
		_ = ws.CloseNow()
		return fmt.Errorf("first frame was %s, want %s", f.Verb, proto.VerbReady)
	}
	c.nick = f.Nick
	return nil
}

// settleTimeout is how long a connection waits to be put in a channel
// before giving up. Nobody speaks until everybody has settled, so without
// this one connection the server forgot about would hold up the whole run
// and print nothing at the end of it.
const settleTimeout = 30 * time.Second

// settle waits until the connection is actually in a channel, and moves it
// to its own if the run is spread over several.
//
// Waiting is not politeness: the server sends READY before it runs the
// autojoin, deliberately, so a client that starts talking the moment it has
// a nick gets ERR notjoined back and the run measures refusals.
func (c *client) settle(ctx context.Context) bool {
	sctx, cancel := context.WithTimeout(ctx, settleTimeout)
	defer cancel()

	ok := c.doSettle(sctx)
	if !ok && ctx.Err() == nil {
		// The connection is still up and it is still nowhere. Nothing else
		// would count this, and it is not a detail: it means the server
		// answered the handshake and then did not finish the job.
		c.stats.closedEarly("never joined")
	}
	return ok
}

func (c *client) doSettle(ctx context.Context) bool {
	def, ok := c.waitJoin(ctx, "")
	if !ok {
		return false
	}
	if c.channel == "" {
		c.channel = def
		return true
	}

	if !c.send(ctx, proto.Command{Verb: proto.VerbJoin, Channel: c.channel}) {
		return false
	}
	if _, ok := c.waitJoin(ctx, c.channel); !ok {
		return false
	}
	// And leave the one the server put us in. The point of spreading a run
	// over several rooms is that they are separate fan-outs; leaving
	// everybody in one big room as well measures the big room.
	return c.send(ctx, proto.Command{Verb: proto.VerbPart, Channel: def})
}

// waitJoin blocks until this connection is told it joined want, or any
// channel if want is empty.
func (c *client) waitJoin(ctx context.Context, want string) (string, bool) {
	for {
		select {
		case <-ctx.Done():
			return "", false
		case got := <-c.joins:
			if want == "" || got == want {
				return got, true
			}
		}
	}
}

func (c *client) speak(ctx context.Context) {
	// Everybody pings. A connection that only listens sends nothing at all,
	// and the server closes those.
	ping := time.NewTicker(c.cfg.PingEvery)
	defer ping.Stop()

	var msgs <-chan time.Time
	if c.speaks {
		period := time.Duration(float64(time.Second) / c.cfg.Rate)

		// Jittered, or every speaker fires on the same instant and the
		// server is measured on a sawtooth no real room produces.
		select {
		case <-time.After(rand.N(period + 1)):
		case <-ctx.Done():
			return
		}

		t := time.NewTicker(period)
		defer t.Stop()
		msgs = t.C
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ping.C:
			if !c.send(ctx, proto.Command{Verb: proto.VerbPing}) {
				return
			}
		case <-msgs: // nil, and so blocks forever, for a listener
			cmd := proto.Command{Verb: proto.VerbMsg, Channel: c.channelArg(), Data: c.payload()}
			if !c.send(ctx, cmd) {
				return
			}
			c.stats.sent.Add(1)
			c.stats.sentIn[c.index%c.cfg.Channels].Add(1)
		}
	}
}

// channelArg is the channel a MSG names. With one channel it names none: an
// empty channel means the server's default, and exercising that is the point
// of the default existing.
func (c *client) channelArg() string {
	if c.cfg.Channels == 1 {
		return ""
	}
	return c.channel
}

// payload is a message body with the send time at the front.
//
// The clock is the same one every receiver reads, because they are all in
// this process, so a receiver can price the whole path — encode, fan out,
// socket, decode — without the two ends having to agree about anything.
func (c *client) payload() string {
	head := strconv.FormatInt(time.Now().UnixNano(), 10) + "|"
	if len(head) >= c.cfg.MessageSize {
		return head
	}
	return head + c.pad[:c.cfg.MessageSize-len(head)]
}

func latencyOf(data string) (time.Duration, bool) {
	i := strings.IndexByte(data, '|')
	if i <= 0 {
		return 0, false
	}
	ns, err := strconv.ParseInt(data[:i], 10, 64)
	if err != nil {
		return 0, false
	}
	return time.Duration(time.Now().UnixNano() - ns), true
}

// read is the measuring end. Every frame the server sends lands here.
func (c *client) read(ctx context.Context) {
	for {
		var f frame
		if err := c.readFrame(ctx, &f); err != nil {
			if ctx.Err() == nil {
				// Not our doing. A lag drop arrives here as a close frame
				// saying so, which is the number that matters most in a
				// fan-out benchmark.
				c.stats.closedEarly(reasonOf(err))
			}
			return
		}

		switch f.Verb {
		case "": // already counted as garbage
		case proto.VerbMsg:
			c.stats.received.Add(1)
			if d, ok := latencyOf(f.Data); ok {
				c.lat.Record(d)
			}
		case proto.VerbErr:
			c.stats.refused(f.Description)
		case proto.VerbJoin:
			c.stats.other.Add(1)
			if f.Nick == c.nick {
				select {
				case c.joins <- f.Channel:
				default: // settle is not listening any more, and is done
				}
			}
		default:
			c.stats.other.Add(1)
		}
	}
}

func (c *client) readFrame(ctx context.Context, f *frame) error {
	typ, data, err := c.ws.Read(ctx)
	if err != nil {
		return err
	}
	if (typ == websocket.MessageBinary) != c.codec.binary {
		c.stats.garbage.Add(1)
		return nil // wrong framing is the server's bug; keep reading
	}
	if err := c.codec.unmarshal(data, f); err != nil {
		c.stats.garbage.Add(1)
		f.Verb = ""
	}
	return nil
}

func (c *client) send(ctx context.Context, cmd proto.Command) bool {
	data, err := c.codec.marshal(cmd)
	if err != nil {
		c.stats.sendFailed.Add(1)
		return false
	}

	typ := websocket.MessageText
	if c.codec.binary {
		typ = websocket.MessageBinary
	}

	wctx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	if err := c.ws.Write(wctx, typ, data); err != nil {
		if ctx.Err() == nil {
			c.stats.sendFailed.Add(1)
		}
		return false
	}
	return true
}

// reasonOf turns an error into something worth counting. A close frame
// becomes its code and reason — "StatusPolicyViolation: too slow" is the
// server telling us exactly what it did.
//
// Everything else has to collapse onto a CLOSED SET of strings, which is
// the same rule the server applies to its own metric labels and for the
// same reason. A net.OpError prints the peer address, so the first run at
// three thousand connections ended in a report with one "losses" entry per
// dropped socket, each with a port number in it. The error inside the
// OpError says what happened without saying to whom.
func reasonOf(err error) string {
	if ce, ok := errors.AsType[websocket.CloseError](err); ok {
		if ce.Reason == "" {
			return ce.Code.String()
		}
		return ce.Code.String() + ": " + ce.Reason
	}

	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timed out"
	case errors.Is(err, context.Canceled):
		return "cancelled"
	case errors.Is(err, net.ErrClosed):
		return "connection closed"
	case errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, io.EOF):
		return "eof"
	}

	var oe *net.OpError
	if errors.As(err, &oe) && oe.Err != nil {
		return oe.Err.Error()
	}
	return err.Error()
}

// frame is every field the load generator looks at, across every kind of
// frame the server sends.
//
// One struct and one decode, which is the same trick the server plays with
// proto.Command on the way in: a client holding ten thousand sockets cannot
// afford to look at a frame twice either. Fields it does not know about are
// ignored by both codecs.
type frame struct {
	Verb        string `json:"verb" msgpack:"verb"`
	Nick        string `json:"nick,omitempty" msgpack:"nick,omitempty"`
	Channel     string `json:"channel,omitempty" msgpack:"channel,omitempty"`
	Data        string `json:"data,omitempty" msgpack:"data,omitempty"`
	Description string `json:"description,omitempty" msgpack:"description,omitempty"`
}

// codec is the client side of the wire format.
//
// It is not proto.Codec, and cannot be: that interface encodes what the
// SERVER sends — Encode takes a proto.Outbound, which a command from a
// client is not. Both directions of both formats are still one Marshal and
// one Unmarshal, so this is the whole of it.
type codec struct {
	name      string
	binary    bool
	marshal   func(any) ([]byte, error)
	unmarshal func([]byte, any) error
}

func codecByName(name string) (codec, error) {
	switch name {
	case "", proto.NameJSON:
		// Wrapped rather than passed straight in: encoding/json/v2 takes
		// trailing options, so its functions do not have this shape.
		return codec{proto.NameJSON, false,
			func(v any) ([]byte, error) { return json.Marshal(v) },
			func(frame []byte, v any) error { return json.Unmarshal(frame, v) },
		}, nil
	case proto.NameMsgPack:
		return codec{proto.NameMsgPack, true, msgpack.Marshal, msgpack.Unmarshal}, nil
	}
	return codec{}, fmt.Errorf("unknown codec %q, want %s or %s", name, proto.NameJSON, proto.NameMsgPack)
}
