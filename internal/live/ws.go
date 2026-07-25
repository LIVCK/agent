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
	// controlQueue buffers inbound control frames between the read pump and the
	// streamer's loop. The pump must never block on the streamer (it also drives
	// ping/pong and closure detection), so a full queue drops the newest frame
	// instead of waiting: pulse repeats stream_on every 15 s, so a dropped frame
	// costs at most one keepalive round, never correctness.
	controlQueue = 8
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
	gc := &gorillaConn{c: c, msgs: make(chan []byte, controlQueue), done: make(chan struct{})}
	gc.start()
	return gc, nil
}

// gorillaConn wraps a *websocket.Conn. A background read pump drives ping/pong
// (so pulse's keepalive is answered and the read deadline is refreshed), hands
// inbound control frames to the streamer and detects closure. Writes happen only
// on the streamer's single goroutine, and the ping handler's WriteControl is
// concurrency-safe with WriteMessage, so no write mutex is needed.
type gorillaConn struct {
	c    *websocket.Conn
	msgs chan []byte
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

// readPump drains inbound frames until the peer closes or errors. It services
// ping/pong, forwards TEXT payloads (pulse's control frames) to the streamer and
// closes msgs on the way out.
//
// It is the ONLY goroutine that sends on msgs and the only one that closes it,
// so the non-blocking send can never race the close — Close() from the streamer
// side merely kills the socket, which makes ReadMessage fail and lets this
// goroutine do the closing.
func (gc *gorillaConn) readPump() {
	defer func() {
		close(gc.msgs)
		gc.markClosed()
	}()
	for {
		mt, b, err := gc.c.ReadMessage()
		if err != nil {
			return
		}
		_ = gc.c.SetReadDeadline(time.Now().Add(pongWait))
		if mt != websocket.TextMessage {
			continue
		}
		select {
		case gc.msgs <- b:
		default: // Streamer busy: drop, see controlQueue.
		}
	}
}

// Recv exposes the inbound control frames. The channel is closed when the
// connection dies, which is how a session that is currently NOT bursting learns
// the peer is gone: it never writes, so it would never see errConnClosed.
func (gc *gorillaConn) Recv() <-chan []byte { return gc.msgs }

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
