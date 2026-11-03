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

// Labelled so a profile can settle whether the latency is the measuring.
const labelTask = "task"

const (
	taskBarrier  = "barrier"
	taskDialer   = "dialer"
	taskClient   = "client"
	taskReader   = "reader"
	taskProgress = "progress"
)

type Config struct {
	URL string

	Conns int

	SpeakPercent float64

	Rate float64

	// Includes the send timestamp at the front; the server refuses over MaxMessage.
	MessageSize int

	// Round robin; one leaves everybody where the server put them.
	Channels int

	// Starts once everybody is connected; zero runs until cancelled.
	Duration time.Duration

	// Ten thousand simultaneous handshakes would measure the listen backlog.
	Ramp time.Duration

	// A listener sends nothing else, and the server closes silent connections.
	PingEvery time.Duration

	Codec string

	Progress time.Duration
	Out      io.Writer
}

// Without it a stalled server hangs the run, and it reports nothing.
const writeTimeout = 10 * time.Second

// Generous: a BACKLOG is the whole replay window in one frame.
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

func (c Config) speakers() int { return int(float64(c.Conns) * c.SpeakPercent / 100) }

// The remainder goes one per channel, so rooms differ by at most one speaker.
func (c Config) speakersIn(i int) int {
	return (c.speakers() - i + c.Channels - 1) / c.Channels
}

// Chosen within its own channel: every Nth overall aliases with the round robin.
func (c Config) speaks(i int) bool {
	ch := i % c.Channels
	pos := i / c.Channels
	members, speakers := c.membersIn(ch), c.speakersIn(ch)
	return pos*speakers/members != (pos+1)*speakers/members
}

func (c Config) membersIn(i int) int {
	return (c.Conns - i + c.Channels - 1) / c.Channels
}

// Empty for a single channel: the server's autojoin is the room.
func (c Config) channelName(i int) string {
	if c.Channels == 1 {
		return ""
	}
	return fmt.Sprintf("bench%d", i%c.Channels)
}

// Run counts failed connections rather than aborting on them.
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

	// Nobody speaks until all are connected, or the ratio measures the ramp.
	var settling sync.WaitGroup
	settling.Add(cfg.Conns)
	start := make(chan struct{})
	go pprof.Do(runCtx, pprof.Labels(labelTask, taskBarrier), func(context.Context) {
		settling.Wait()
		close(start)
	})

	var wg sync.WaitGroup
	wg.Add(cfg.Conns)
	// Shadowed on purpose: everything dialled below must inherit the labels.
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

type client struct {
	cfg   Config
	stats *stats
	codec codec
	pad   string

	index   int
	speaks  bool
	channel string // empty until settle learns the default

	ws   *websocket.Conn
	nick string
	lat  Histogram

	// Only this connection's own JOINs: a big room announces thousands more.
	joins chan string
}

func (c *client) run(ctx context.Context, settling *sync.WaitGroup, start <-chan struct{}) {
	// Exactly once, whatever happens, or one failed dial holds the run hostage.
	settled := sync.OnceFunc(settling.Done)
	defer settled()

	if err := c.dial(ctx); err != nil {
		c.stats.dialErr(reasonOf(err))
		return
	}
	defer func() { _ = c.ws.CloseNow() }()
	c.stats.dialed.Add(1)

	connCtx, dead := context.WithCancel(ctx)

	done := make(chan struct{})
	go func() {
		defer close(done)
		defer dead()
		pprof.Do(connCtx, pprof.Labels(labelTask, taskReader), c.read)
	}()

	// Run in reverse: the histogram is touched once the reader has stopped.
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

	// The reader needs the nick from READY to tell its own JOIN from everyone's.
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

// Without it one connection the server forgot holds up the whole run.
const settleTimeout = 30 * time.Second

// READY precedes the autojoin: talking at once would only get ERR notjoined.
func (c *client) settle(ctx context.Context) bool {
	sctx, cancel := context.WithTimeout(ctx, settleTimeout)
	defer cancel()

	ok := c.doSettle(sctx)
	if !ok && ctx.Err() == nil {
		// Up but in no channel: nothing else would count the server failing this.
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
	// Leave the autojoined room too, or every run also measures one big room.
	return c.send(ctx, proto.Command{Verb: proto.VerbPart, Channel: def})
}

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
	ping := time.NewTicker(c.cfg.PingEvery)
	defer ping.Stop()

	var msgs <-chan time.Time
	if c.speaks {
		period := time.Duration(float64(time.Second) / c.cfg.Rate)

		// Jittered, or every speaker fires at once: a sawtooth no real room makes.
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

// Names none for a single channel, so the default channel gets exercised.
func (c *client) channelArg() string {
	if c.cfg.Channels == 1 {
		return ""
	}
	return c.channel
}

// Sender and receivers share this process's clock, so latency needs no sync.
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

func (c *client) read(ctx context.Context) {
	for {
		var f frame
		if err := c.readFrame(ctx, &f); err != nil {
			if ctx.Err() == nil {
				// Not ours: a lag drop is a close frame, the number that matters most.
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

// A closed set: an OpError's text names the peer, one entry per socket.
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

// One struct for every frame, so each costs one decode, as proto.Command does.
type frame struct {
	Verb        string `json:"verb" msgpack:"verb"`
	Nick        string `json:"nick,omitempty" msgpack:"nick,omitempty"`
	Channel     string `json:"channel,omitempty" msgpack:"channel,omitempty"`
	Data        string `json:"data,omitempty" msgpack:"data,omitempty"`
	Description string `json:"description,omitempty" msgpack:"description,omitempty"`
}

// Not proto.Codec, which only encodes what the server sends.
type codec struct {
	name      string
	binary    bool
	marshal   func(any) ([]byte, error)
	unmarshal func([]byte, any) error
}

func codecByName(name string) (codec, error) {
	switch name {
	case "", proto.NameJSON:
		// Wrapped: json/v2's functions take trailing options, so differ in shape.
		return codec{proto.NameJSON, false,
			func(v any) ([]byte, error) { return json.Marshal(v) },
			func(frame []byte, v any) error { return json.Unmarshal(frame, v) },
		}, nil
	case proto.NameMsgPack:
		return codec{proto.NameMsgPack, true, msgpack.Marshal, msgpack.Unmarshal}, nil
	}
	return codec{}, fmt.Errorf("unknown codec %q, want %s or %s", name, proto.NameJSON, proto.NameMsgPack)
}
