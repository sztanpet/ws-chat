package main

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/sztanpet/ws-chat/internal/config"
	"github.com/sztanpet/ws-chat/internal/proto"
)

func TestHealth(t *testing.T) {
	ta := newTestApp(t)
	resp, err := ta.srv.Client().Get(ta.srv.URL + "/health")
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

func TestBroadcast(t *testing.T) {
	ta := newTestApp(t)
	alice, bob := ta.dial(t), ta.dial(t)

	alice.send(proto.Command{Verb: proto.VerbMsg, Data: "hello everyone"})

	// The sender hears itself too: one stream to render, not two to reconcile.
	for _, c := range []*client{alice, bob} {
		msg := c.expectMsg("", "hello everyone")
		if msg.ID == 0 {
			t.Error("server assigned no message id")
		}
		if msg.Timestamp == 0 {
			t.Error("server assigned no timestamp")
		}
		if msg.Nick == "" {
			t.Error("message has no nick")
		}
	}
}

func TestMessageIDsAreMonotonic(t *testing.T) {
	ta := newTestApp(t)
	alice, bob := ta.dial(t), ta.dial(t)

	alice.send(proto.Command{Verb: proto.VerbMsg, Data: "one"})
	first := alice.expectMsg("", "one")
	bob.expectMsg("", "one")

	bob.send(proto.Command{Verb: proto.VerbMsg, Data: "two"})
	second := alice.expectMsg("", "two")

	if second.ID <= first.ID {
		t.Fatalf("ids went %d then %d, want increasing", first.ID, second.ID)
	}
}

func TestNickIsAssignedByServer(t *testing.T) {
	ta := newTestApp(t)
	alice, bob := ta.dial(t), ta.dial(t)

	alice.sendRaw([]byte(`{"verb":"MSG","data":"hi","nick":"root"}`), websocket.MessageText)
	msg := alice.expectMsg("", "hi")
	if msg.Nick != alice.nick {
		t.Fatalf("nick = %q, want the assigned %q", msg.Nick, alice.nick)
	}
	if other := bob.expectMsg("", "hi"); other.Nick != msg.Nick {
		t.Fatalf("the two copies disagree on the sender: %q and %q", msg.Nick, other.Nick)
	}
}

func TestPing(t *testing.T) {
	ta := newTestApp(t)
	c := ta.dial(t)

	c.send(proto.Command{Verb: proto.VerbPing})
	if verb, _ := c.recv(); verb != proto.VerbPong {
		t.Fatalf("got %s, want %s", verb, proto.VerbPong)
	}
}

func TestProtocolErrors(t *testing.T) {
	tests := []struct {
		name  string
		frame string
		want  string
	}{
		{"unknown verb", `{"verb":"FLARP","data":"x"}`, proto.ErrUnknown},
		{"no verb", `{"data":"x"}`, proto.ErrProtocol},
		{"wrong case", `{"verb":"msg","data":"x"}`, proto.ErrUnknown},
		{"truncated", `{"verb":"MSG","data":`, proto.ErrProtocol},
		{"not a document", `MSG {"data":"x"}`, proto.ErrProtocol},
		{"not json at all", `hello`, proto.ErrProtocol},
		{"an array", `["MSG","x"]`, proto.ErrProtocol},
		{"verb but no body", `{"verb":"MSG"}`, proto.ErrEmpty},
		{"empty message", `{"verb":"MSG","data":""}`, proto.ErrEmpty},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ta := newTestApp(t)
			c := ta.dial(t)
			c.sendRaw([]byte(tt.frame), websocket.MessageText)
			c.expectErr(tt.want)
		})
	}
}

func TestMessageTooLong(t *testing.T) {
	ta := newTestApp(t, func(c *config.Config) { c.MaxMessage = 8 })
	c := ta.dial(t)

	c.send(proto.Command{Verb: proto.VerbMsg, Data: "123456789"})
	c.expectErr(proto.ErrTooLong)

	c.send(proto.Command{Verb: proto.VerbMsg, Data: "12345678"})
	c.expectMsg("", "12345678")
}

func TestErrorsDoNotCloseTheConnection(t *testing.T) {
	ta := newTestApp(t)
	c := ta.dial(t)

	c.sendRaw([]byte(`{"verb":"FLARP"}`), websocket.MessageText)
	c.expectErr(proto.ErrUnknown)

	c.send(proto.Command{Verb: proto.VerbMsg, Data: "still here"})
	c.expectMsg("", "still here")
}

func TestWrongFramingRejected(t *testing.T) {
	ta := newTestApp(t)
	c := ta.dial(t) // JSON, so text

	c.sendRaw([]byte(`{"verb":"MSG","data":"x"}`), websocket.MessageBinary)
	c.expectErr(proto.ErrFraming)
}

func TestPrivateMessage(t *testing.T) {
	ta := newTestApp(t)
	alice, bob, carol := ta.dial(t), ta.dial(t), ta.dial(t)

	alice.send(proto.Command{Verb: proto.VerbPriv, Nick: bob.nick, Data: "just for you"})

	got := bob.expectPriv("just for you")
	if got.Nick != alice.nick {
		t.Fatalf("recipient sees nick %q, want the sender %q", got.Nick, alice.nick)
	}
	if got.Sent {
		t.Error("recipient's copy is marked as sent")
	}

	echo := alice.expectPriv("just for you")
	if echo.Nick != bob.nick {
		t.Fatalf("sender's echo names %q, want the recipient %q", echo.Nick, bob.nick)
	}
	if !echo.Sent {
		t.Error("sender's echo is not marked as sent")
	}
	if echo.ID != got.ID {
		t.Errorf("the two copies have different ids: %d and %d", echo.ID, got.ID)
	}

	// Carol proves absence by reading the next broadcast: a leak would come first.
	alice.send(proto.Command{Verb: proto.VerbMsg, Data: "public again"})
	carol.expectMsg("", "public again")
}

func TestPrivateMessageErrors(t *testing.T) {
	ta := newTestApp(t)

	t.Run("no such nick", func(t *testing.T) {
		c := ta.dial(t)
		c.send(proto.Command{Verb: proto.VerbPriv, Nick: "nobody", Data: "hello?"})
		c.expectErr(proto.ErrNoSuch)
	})

	t.Run("to self", func(t *testing.T) {
		c := ta.dial(t)
		c.send(proto.Command{Verb: proto.VerbPriv, Nick: c.nick, Data: "hello me"})
		c.expectErr(proto.ErrSelf)
	})

	t.Run("empty", func(t *testing.T) {
		c := ta.dial(t)
		c.send(proto.Command{Verb: proto.VerbPriv, Nick: "someone", Data: ""})
		c.expectErr(proto.ErrEmpty)
	})

	t.Run("bad json", func(t *testing.T) {
		c := ta.dial(t)
		c.sendRaw([]byte(`PRIVMSG nonsense`), websocket.MessageText)
		c.expectErr(proto.ErrProtocol)
	})
}

// Unit-tested: whether a socket's queue fills depends on kernel buffers.
func TestDeliverRefusesWhenFull(t *testing.T) {
	c := &conn{priv: make(chan []byte, 2)}

	if !c.deliver([]byte("one")) {
		t.Fatal("first deliver refused with an empty queue")
	}
	if !c.deliver([]byte("two")) {
		t.Fatal("second deliver refused with room to spare")
	}

	done := make(chan bool, 1)
	go func() { done <- c.deliver([]byte("three")) }()
	select {
	case ok := <-done:
		if ok {
			t.Fatal("deliver accepted a third message into a queue of two")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("deliver blocked on a full queue instead of refusing")
	}
}

func TestPrivateMessageToSilentRecipientDoesNotStallSender(t *testing.T) {
	ta := newTestApp(t, func(c *config.Config) {
		c.PrivBuffer = 1
		c.WriteTimeout = config.Duration(200 * time.Millisecond)
	})

	alice := ta.dial(t)
	silent := ta.dial(t) // never reads again after this

	pm := proto.Command{Verb: proto.VerbPriv, Nick: silent.nick, Data: "anyone there"}
	deadline := time.Now().Add(20 * time.Second)
	for range 50 {
		alice.send(pm)
		verb, payload := alice.recv() // fails the test if it takes >5s

		if verb == proto.VerbErr {
			// Any answer will do, as long as it is not a wait.
			var e proto.Err
			mustUnmarshal(t, alice, payload, &e)
			switch e.Description {
			case proto.ErrBacklog, proto.ErrNoSuch:
			default:
				t.Fatalf("ERR %q, want %q or %q", e.Description, proto.ErrBacklog, proto.ErrNoSuch)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("sender is still waiting on a recipient that stopped reading")
		}
	}
}

// Driven from inside: no client can make two writers overlap on demand.
func TestWriteDeadlineOutlivesTheFirstWriter(t *testing.T) {
	ta := newTestApp(t, func(c *config.Config) {
		c.WriteTimeout = config.Duration(200 * time.Millisecond)
	})
	silent := ta.dial(t) // never reads again after this
	c, ok := ta.app.lookup(silent.nick)
	if !ok {
		t.Fatal("connection is not in the directory")
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		frame := make([]byte, 64<<10)
		for {
			c.cork()
			c.cork()
			c.uncork(nil)
			if !c.uncork(c.writeTo(frame)) {
				return
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a write to a client that stopped reading outlived its deadline")
	}
}

func TestPumpsShareOneSocket(t *testing.T) {
	ta := newTestApp(t)
	obs := ta.dial(t)

	const rooms, n = 4, 200
	var senders []*client
	for i := range rooms {
		room := fmt.Sprint("room", i)
		s := ta.dial(t)
		s.send(proto.Command{Verb: proto.VerbJoin, Channel: room})
		s.expectJoined(room)
		obs.send(proto.Command{Verb: proto.VerbJoin, Channel: room})
		obs.expectJoined(room)
		senders = append(senders, s)
	}

	errs := make(chan error, rooms)
	for i, s := range senders {
		var frames [][]byte
		for j := range n {
			frames = append(frames, s.encode(proto.Command{Verb: proto.VerbMsg, Channel: fmt.Sprint("room", i), Data: fmt.Sprint(j)}))
		}
		go func() {
			for _, frame := range frames {
				if err := s.ws.Write(context.Background(), s.msgType(), frame); err != nil {
					errs <- err
					return
				}
			}
			errs <- nil
		}()
	}
	for range senders {
		if err := <-errs; err != nil {
			t.Fatalf("send: %v", err)
		}
	}

	next := map[string]int{}
	for range rooms * n {
		msg := obs.expectMsg("", "")
		if want := fmt.Sprint(next[msg.Channel]); msg.Data != want {
			t.Fatalf("%s: got %q, want %q", msg.Channel, msg.Data, want)
		}
		next[msg.Channel]++
	}
}

func TestShutdownClosesConnections(t *testing.T) {
	ta := newTestApp(t)
	c := ta.dial(t)

	c.send(proto.Command{Verb: proto.VerbMsg, Data: "still up"})
	c.expectMsg("", "still up")

	ta.app.close()

	if code := c.expectClosed(); code != websocket.StatusGoingAway {
		t.Fatalf("closed with %v, want %v", code, websocket.StatusGoingAway)
	}
}

// The rest of the suite is JSON; these run the core over every codec.
func TestEveryCodecDelivers(t *testing.T) {
	for _, codec := range proto.Codecs() {
		t.Run(codec.Name(), func(t *testing.T) {
			ta := newTestApp(t)
			alice := ta.dialCodec(t, codec)
			bob := ta.dialCodec(t, codec)

			alice.send(proto.Command{Verb: proto.VerbMsg, Data: "hello over " + codec.Name()})
			for _, c := range []*client{alice, bob} {
				msg := c.expectMsg(alice.nick, "hello over "+codec.Name())
				if msg.ID == 0 || msg.Timestamp == 0 {
					t.Error("server assigned no id or timestamp")
				}
			}

			alice.send(proto.Command{Verb: proto.VerbPriv, Nick: bob.nick, Data: "privately"})
			if got := bob.expectPriv("privately"); got.Nick != alice.nick {
				t.Errorf("recipient sees %q, want %q", got.Nick, alice.nick)
			}
			alice.expectPriv("privately")

			alice.send(proto.Command{Verb: proto.VerbPing})
			if verb, _ := alice.recv(); verb != proto.VerbPong {
				t.Errorf("got %s, want %s", verb, proto.VerbPong)
			}

			alice.send(proto.Command{Verb: proto.VerbMsg, Data: ""})
			alice.expectErr(proto.ErrEmpty)
		})
	}
}

// Catches a message fanned out in the sender's format to everybody.
func TestCodecsMixInOneRoom(t *testing.T) {
	ta := newTestApp(t)
	jsonClient := ta.dialCodec(t, proto.JSON{})
	packClient := ta.dialCodec(t, proto.MsgPack{})

	jsonClient.send(proto.Command{Verb: proto.VerbMsg, Data: "everyone hears this"})
	first := jsonClient.expectMsg(jsonClient.nick, "everyone hears this")
	second := packClient.expectMsg(jsonClient.nick, "everyone hears this")
	if first.ID != second.ID {
		t.Fatalf("the two copies have different ids: %d and %d", first.ID, second.ID)
	}

	packClient.send(proto.Command{Verb: proto.VerbMsg, Data: "so do they"})
	jsonClient.expectMsg(packClient.nick, "so do they")
	packClient.expectMsg(packClient.nick, "so do they")

	jsonClient.send(proto.Command{Verb: proto.VerbPriv, Nick: packClient.nick, Data: "just you"})
	if got := packClient.expectPriv("just you"); got.Nick != jsonClient.nick {
		t.Fatalf("recipient sees %q, want %q", got.Nick, jsonClient.nick)
	}
	jsonClient.expectPriv("just you")
}

func TestNoSubprotocolGetsTheDefault(t *testing.T) {
	ta := newTestApp(t)

	ctx, cancel := contextWithTimeout()
	defer cancel()

	url := "ws" + strings.TrimPrefix(ta.srv.URL, "http") + "/ws"
	ws, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = ws.CloseNow() }()

	if got := ws.Subprotocol(); got != "" {
		t.Fatalf("negotiated %q, want none", got)
	}

	c := &client{t: t, ws: ws, codec: proto.Default()}
	if verb, _ := c.recv(); verb != proto.VerbReady {
		t.Fatalf("first frame was %s, want %s", verb, proto.VerbReady)
	}
}

func TestOrderIsConsistentAcrossCodecs(t *testing.T) {
	ta := newTestApp(t)
	jsonClient := ta.dialCodec(t, proto.JSON{})
	packClient := ta.dialCodec(t, proto.MsgPack{})

	const n = 20
	for i := range n {
		jsonClient.send(proto.Command{Verb: proto.VerbMsg, Data: fmt.Sprintf("message %d", i)})
	}

	var jsonIDs, packIDs []uint64
	for i := range n {
		want := fmt.Sprintf("message %d", i)
		jsonIDs = append(jsonIDs, jsonClient.expectMsg("", want).ID)
		packIDs = append(packIDs, packClient.expectMsg("", want).ID)
	}
	if !slices.Equal(jsonIDs, packIDs) {
		t.Fatalf("the two clients saw different id orders:\n%v\n%v", jsonIDs, packIDs)
	}
	if !slices.IsSorted(jsonIDs) {
		t.Fatalf("ids arrived out of order: %v", jsonIDs)
	}
}

// Concurrent on purpose: a single sender, as above, passes with no lock at all.
func TestOrderHoldsUnderConcurrentSenders(t *testing.T) {
	ta := newTestApp(t)
	jsonObs := ta.dialCodec(t, proto.JSON{})
	packObs := ta.dialCodec(t, proto.MsgPack{})

	var senders, elsewhere []*client
	for i := range 4 {
		var codec proto.Codec = proto.JSON{}
		if i%2 == 1 {
			codec = proto.MsgPack{}
		}
		senders = append(senders, ta.dialCodec(t, codec))
	}
	for range 2 {
		c := ta.dial(t)
		c.send(proto.Command{Verb: proto.VerbJoin, Channel: "elsewhere"})
		c.expectJoined("elsewhere")
		elsewhere = append(elsewhere, c)
	}

	const n = 50
	type job struct {
		c      *client
		frames [][]byte
	}
	var jobs []job
	for i, c := range senders {
		var frames [][]byte
		for j := range n {
			frames = append(frames, c.encode(proto.Command{Verb: proto.VerbMsg, Data: fmt.Sprintf("%d/%d", i, j)}))
		}
		jobs = append(jobs, job{c, frames})
	}
	for _, c := range elsewhere {
		var frames [][]byte
		for range n {
			frames = append(frames, c.encode(proto.Command{Verb: proto.VerbMsg, Channel: "elsewhere", Data: "noise"}))
		}
		jobs = append(jobs, job{c, frames})
	}

	errs := make(chan error, len(jobs))
	for _, j := range jobs {
		go func() {
			for _, frame := range j.frames {
				if err := j.c.ws.Write(context.Background(), j.c.msgType(), frame); err != nil {
					errs <- err
					return
				}
			}
			errs <- nil
		}()
	}
	for range jobs {
		if err := <-errs; err != nil {
			t.Fatalf("send: %v", err)
		}
	}

	ids := func(c *client) []uint64 {
		var got []uint64
		for range len(senders) * n {
			got = append(got, c.expectMsg("", "").ID)
		}
		return got
	}
	jsonIDs, packIDs := ids(jsonObs), ids(packObs)
	if !slices.Equal(jsonIDs, packIDs) {
		t.Fatalf("the two codecs saw different orders:\n%v\n%v", jsonIDs, packIDs)
	}
	if !slices.IsSorted(jsonIDs) {
		t.Fatalf("ids arrived out of order: %v", jsonIDs)
	}
}
