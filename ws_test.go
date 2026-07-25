package main

import (
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

// The walking skeleton, end to end: two clients connect, one talks, both
// hear it.
func TestBroadcast(t *testing.T) {
	ta := newTestApp(t)
	alice, bob := ta.dial(t), ta.dial(t)

	alice.send(`MSG {"data":"hello everyone"}`)

	// The sender sees its own message too — it is a broadcast, not an echo,
	// and a client should render one stream rather than reconcile two.
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

// Message ids come from the server and are monotonic, whoever sent them.
func TestMessageIDsAreMonotonic(t *testing.T) {
	ta := newTestApp(t)
	alice, bob := ta.dial(t), ta.dial(t)

	alice.send(`MSG {"data":"one"}`)
	first := alice.expectMsg("", "one")
	bob.expectMsg("", "one")

	bob.send(`MSG {"data":"two"}`)
	second := alice.expectMsg("", "two")

	if second.ID <= first.ID {
		t.Fatalf("ids went %d then %d, want increasing", first.ID, second.ID)
	}
}

// Nicks are the server's to assign, and a client cannot claim someone
// else's by putting one in the payload.
func TestNickIsAssignedByServer(t *testing.T) {
	ta := newTestApp(t)
	alice, bob := ta.dial(t), ta.dial(t)

	alice.send(`MSG {"data":"hi","nick":"root"}`)
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

	c.send(proto.VerbPing)
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
		{"unknown verb", `FLARP {"data":"x"}`, proto.ErrUnknown},
		{"not a frame", `{"data":"x"}`, proto.ErrProtocol},
		{"lowercase verb", `msg {"data":"x"}`, proto.ErrProtocol},
		{"bad json", `MSG {"data":`, proto.ErrProtocol},
		{"no payload", `MSG`, proto.ErrProtocol},
		{"empty message", `MSG {"data":""}`, proto.ErrEmpty},
		{"trailing space", `MSG `, proto.ErrProtocol},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ta := newTestApp(t)
			c := ta.dial(t)
			c.send(tt.frame)
			c.expectErr(tt.want)
		})
	}
}

func TestMessageTooLong(t *testing.T) {
	ta := newTestApp(t, func(c *config.Config) { c.MaxMessage = 8 })
	c := ta.dial(t)

	c.send(`MSG {"data":"123456789"}`)
	c.expectErr(proto.ErrTooLong)

	// The boundary itself is allowed.
	c.send(`MSG {"data":"12345678"}`)
	c.expectMsg("", "12345678")
}

// A bad frame is a refusal, not a disconnect: one typo from a buggy client
// should not cost it the connection.
func TestErrorsDoNotCloseTheConnection(t *testing.T) {
	ta := newTestApp(t)
	c := ta.dial(t)

	c.send(`FLARP`)
	c.expectErr(proto.ErrUnknown)

	c.send(`MSG {"data":"still here"}`)
	c.expectMsg("", "still here")
}

func TestBinaryFrameRejected(t *testing.T) {
	ta := newTestApp(t)
	c := ta.dial(t)

	ctx, cancel := contextWithTimeout()
	defer cancel()
	if err := c.ws.Write(ctx, websocket.MessageBinary, []byte("MSG {}")); err != nil {
		t.Fatalf("write: %v", err)
	}
	c.expectErr(proto.ErrBinary)
}

func TestPrivateMessage(t *testing.T) {
	ta := newTestApp(t)
	alice, bob, carol := ta.dial(t), ta.dial(t), ta.dial(t)

	alice.send(`PRIVMSG {"nick":"` + bob.nick + `","data":"just for you"}`)

	// The recipient's copy names the sender and is not marked as sent.
	got := bob.expectPriv("just for you")
	if got.Nick != alice.nick {
		t.Fatalf("recipient sees nick %q, want the sender %q", got.Nick, alice.nick)
	}
	if got.Sent {
		t.Error("recipient's copy is marked as sent")
	}

	// The sender's echo names the recipient and is marked as sent.
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

	// Nobody else sees it. Carol is made to prove it by receiving the next
	// broadcast instead: if the private message had reached her it would be
	// first in her stream.
	alice.send(`MSG {"data":"public again"}`)
	carol.expectMsg("", "public again")
}

func TestPrivateMessageErrors(t *testing.T) {
	ta := newTestApp(t)

	t.Run("no such nick", func(t *testing.T) {
		c := ta.dial(t)
		c.send(`PRIVMSG {"nick":"nobody","data":"hello?"}`)
		c.expectErr(proto.ErrNoSuch)
	})

	t.Run("to self", func(t *testing.T) {
		c := ta.dial(t)
		c.send(`PRIVMSG {"nick":"` + c.nick + `","data":"hello me"}`)
		c.expectErr(proto.ErrSelf)
	})

	t.Run("empty", func(t *testing.T) {
		c := ta.dial(t)
		c.send(`PRIVMSG {"nick":"someone","data":""}`)
		c.expectErr(proto.ErrEmpty)
	})

	t.Run("bad json", func(t *testing.T) {
		c := ta.dial(t)
		c.send(`PRIVMSG nonsense`)
		c.expectErr(proto.ErrProtocol)
	})
}

// deliver is the one place backpressure could escape into the sender, so
// its refusal is tested directly rather than through a socket: whether a
// real recipient's queue fills depends on kernel buffer sizes and timing,
// and a test that depends on those is a test that fails on someone else's
// machine.
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

// The end-to-end version of the same invariant: a recipient that has
// stopped reading must not stall the person messaging it, whichever way
// the server decides to handle it.
func TestPrivateMessageToSilentRecipientDoesNotStallSender(t *testing.T) {
	ta := newTestApp(t, func(c *config.Config) {
		c.PrivBuffer = 1
		c.WriteTimeout = config.Duration(200 * time.Millisecond)
	})

	alice := ta.dial(t)
	silent := ta.dial(t) // never reads again after this

	pm := `PRIVMSG {"nick":"` + silent.nick + `","data":"anyone there"}`
	deadline := time.Now().Add(20 * time.Second)
	for range 50 {
		alice.send(pm)
		verb, payload := alice.recv() // fails the test if it takes >5s

		if verb == proto.VerbErr {
			// Whatever the reason, the sender was answered rather than
			// left to wait on somebody else's socket.
			var e proto.Err
			mustUnmarshal(t, payload, &e)
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

// Shutting the server down closes the connections. net/http's Shutdown
// ignores hijacked connections, so this is entirely our own doing and worth
// a test.
func TestShutdownClosesConnections(t *testing.T) {
	ta := newTestApp(t)
	c := ta.dial(t)

	c.send(`MSG {"data":"still up"}`)
	c.expectMsg("", "still up")

	ta.app.close()

	if code := c.expectClosed(); code != websocket.StatusGoingAway {
		t.Fatalf("closed with %v, want %v", code, websocket.StatusGoingAway)
	}
}
