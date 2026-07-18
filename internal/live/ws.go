package live

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	// handshakeTimeout bounds the WS upgrade.
	handshakeTimeout = 10 * time.Second
	// writeWait bounds one frame write.
	writeWait = 10 * time.Second
	// pongWait is the read deadline; pulse pings every 25s, so this leaves
	// comfortable margin before a missed ping is read as a dead peer.
	pongWait = 70 * time.Second
	// maxServerMsg caps inbound frames. pulse only sends control frames on this
	// channel, so anything larger is anomalous.
	maxServerMsg = 4 * 1024
)

// errConnClosed is returned by WriteText once the read pump has observed the
// connection die, so the streamer reconnects instead of writing into the void.
var errConnClosed = errors.New("live: connection closed")

// gorillaDialer is the production Dialer.
type gorillaDialer struct{ d *websocket.Dialer }

// NewDialer returns the production WebSocket Dialer.
func NewDialer() Dialer {
	return &gorillaDialer{d: &websocket.Dialer{HandshakeTimeout: handshakeTimeout}}
}

// Dial opens the Bearer-authenticated signal channel and starts its read pump.
func (g *gorillaDialer) Dial(ctx context.Context, url, token string) (Conn, error) {
	h := http.Header{}
	h.Set("Authorization", "Bearer "+token)
	c, resp, err := g.d.DialContext(ctx, url, h)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if err != nil {
		return nil, err
	}
	gc := &gorillaConn{c: c, done: make(chan struct{})}
	gc.start()
	return gc, nil
}

// gorillaConn wraps a *websocket.Conn. A background read pump drives ping/pong
// (so pulse's keepalive is answered and the read deadline is refreshed) and
// detects closure. Writes happen only on the streamer's single goroutine, and
// the ping handler's WriteControl is concurrency-safe with WriteMessage, so no
// write mutex is needed.
type gorillaConn struct {
	c    *websocket.Conn
	done chan struct{}
	once sync.Once
}

func (gc *gorillaConn) start() {
	gc.c.SetReadLimit(maxServerMsg)
	_ = gc.c.SetReadDeadline(time.Now().Add(pongWait))
	gc.c.SetPingHandler(func(string) error {
		_ = gc.c.SetReadDeadline(time.Now().Add(pongWait))
		err := gc.c.WriteControl(websocket.PongMessage, nil, time.Now().Add(writeWait))
		if errors.Is(err, websocket.ErrCloseSent) || errors.Is(err, net.ErrClosed) {
			return nil
		}
		return err
	})
	go gc.readPump()
}

// readPump drains inbound frames until the peer closes or errors. It exists to
// service ping/pong and to observe closure; the payloads pulse sends are not
// used by the agent.
func (gc *gorillaConn) readPump() {
	defer gc.markClosed()
	for {
		if _, _, err := gc.c.ReadMessage(); err != nil {
			return
		}
		_ = gc.c.SetReadDeadline(time.Now().Add(pongWait))
	}
}

// WriteText sends one burst frame. It returns errConnClosed once the read pump
// has seen the connection die.
func (gc *gorillaConn) WriteText(b []byte) error {
	select {
	case <-gc.done:
		return errConnClosed
	default:
	}
	_ = gc.c.SetWriteDeadline(time.Now().Add(writeWait))
	return gc.c.WriteMessage(websocket.TextMessage, b)
}

// Close tears down the connection and stops the read pump.
func (gc *gorillaConn) Close() error {
	gc.markClosed()
	return gc.c.Close()
}

func (gc *gorillaConn) markClosed() { gc.once.Do(func() { close(gc.done) }) }
