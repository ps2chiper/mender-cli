// Copyright 2022 Northern.tech AS
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

package cmd

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/mendersoftware/mender-cli/log"

	"github.com/google/uuid"
	"github.com/mendersoftware/go-lib-micro/ws"
	"github.com/mendersoftware/go-lib-micro/ws/portforward"
	wspf "github.com/mendersoftware/go-lib-micro/ws/portforward"
	"github.com/vmihailenco/msgpack"
)

const (
	portForwardTCPChannelSize = 20
	portForwardTCPWindow      = 8
	// Leave room for ACKs and control frames in Deviceconnect's 25-message channel.
	portForwardV3TCPWindow = 16
	// v2 TCP frames are larger than the v1 4 KiB chunk. The Rust daemon
	// accepts the matching limit for an opted-in v2 connection.
	portForwardTCPReadBuffer  = 7680
	portForwardV3FrameSize    = 32 * 1024
	portForwardV3MaxFrameSize = 256 * 1024
)

// errLocalHalfClose tells handleRequest that the client has finished sending
// but the local accepted connection must stay open for the target's response.
var errLocalHalfClose = errors.New("local half-close")

// halfCloseTracker serializes the two independent TCP half-closes. A v3
// connection is finished only when both directions are closed, or when the
// peer explicitly sends stop.
type halfCloseTracker struct {
	mu       sync.Mutex
	local    bool
	remote   bool
	stopped  bool
	finished bool
	done     chan struct{}
}

func newHalfCloseTracker() *halfCloseTracker {
	return &halfCloseTracker{done: make(chan struct{})}
}

func (t *halfCloseTracker) finishLocked() {
	if t.finished {
		return
	}
	if t.stopped || (t.local && t.remote) {
		t.finished = true
		close(t.done)
	}
}

func (t *halfCloseTracker) markLocal() {
	t.mu.Lock()
	t.local = true
	t.finishLocked()
	t.mu.Unlock()
}

func (t *halfCloseTracker) markRemote() {
	t.mu.Lock()
	t.remote = true
	t.finishLocked()
	t.mu.Unlock()
}

func (t *halfCloseTracker) markStopped() {
	t.mu.Lock()
	t.stopped = true
	t.finishLocked()
	t.mu.Unlock()
}

type TCPPortForwarder struct {
	listen     net.Listener
	remoteHost string
	remotePort uint16

	proto ws.ProtoType
}

func NewTCPPortForwarder(
	bindingHost string,
	localPort uint16,
	remoteHost string,
	remotePort uint16,
	proto ws.ProtoType,
) (*TCPPortForwarder, error) {
	fmt.Printf("Forwarding from %s:%d -> %s:%d\n", bindingHost, localPort, remoteHost, remotePort)
	listen, err := net.Listen(protocolTCP, bindingHost+":"+strconv.Itoa(int(localPort)))
	if err != nil {
		return nil, err
	}
	return &TCPPortForwarder{
		listen:     listen,
		remoteHost: remoteHost,
		remotePort: remotePort,
		proto:      proto,
	}, nil
}

func (p *TCPPortForwarder) Run(
	ctx context.Context,
	sessionID string,
	msgChan chan *ws.ProtoMsg,
	recvChans map[string]forwardInbox,
	recvMu *sync.RWMutex,
) {
	// listen for new connections
	defer p.listen.Close()
	acceptedConnections := make(chan net.Conn)

	// go-routine to accept new connections
	go func() {
		for {
			conn, err := p.listen.Accept()
			if err != nil {
				return
			}
			fmt.Printf(
				"Handling connection from %s to %s\n",
				conn.RemoteAddr().String(),
				conn.LocalAddr().String(),
			)
			select {
			case acceptedConnections <- conn:
			case <-ctx.Done():
				conn.Close()
				return
			}
		}
	}()

	// handle new connections
	for {
		select {
		case conn := <-acceptedConnections:
			if tcpConn, ok := conn.(*net.TCPConn); ok {
				_ = tcpConn.SetReadBuffer(4 * 1024 * 1024)
				_ = tcpConn.SetWriteBuffer(4 * 1024 * 1024)
			}
			connectionUUID, _ := uuid.NewUUID()
			connectionID := connectionUUID.String()
			if isFlowControlledPortForward(p.proto) {
				connectionID += ":flow"
			}
			recvChan := make(chan *ws.ProtoMsg, portForwardTCPChannelSize)
			connCtx, cancel := context.WithCancel(ctx)
			recvMu.Lock()
			recvChans[connectionID] = forwardInbox{recvChan, connCtx.Done()}
			recvMu.Unlock()
			go func() {
				defer func() {
					cancel()
					recvMu.Lock()
					delete(recvChans, connectionID)
					recvMu.Unlock()
				}()
				p.handleRequest(connCtx, conn, sessionID, connectionID, recvChan, msgChan)
			}()
		case <-ctx.Done():
			return
		}
	}
}

// A v3 peer confirms larger frames in its new reply. Earlier peers reply
// with an empty body. Bound outstanding data to 4 MiB for negotiated frames.
func tcpFlowLimits(proto ws.ProtoType, body []byte) (frameSize, window int) {
	switch proto {
	case protoTypePortForwardV3:
		var accept struct {
			TCPFrameSize uint32 `msgpack:"tcp_frame_size"`
		}
		if len(body) > 0 && msgpack.Unmarshal(body, &accept) == nil {
			switch accept.TCPFrameSize {
			case portForwardV3MaxFrameSize, portForwardV3FrameSize:
				return int(accept.TCPFrameSize), portForwardV3TCPWindow
			}
		}
		return portForwardTCPReadBuffer, portForwardV3TCPWindow
	case ws.ProtoTypePortForwardV2:
		return portForwardTCPReadBuffer, portForwardTCPWindow
	default:
		return readBuffLength, 1
	}
}

// go routine to handle received messages
func (p *TCPPortForwarder) handleDeviceMessages(
	ctx context.Context,
	msgChan chan<- *ws.ProtoMsg,
	dataChan chan<- []byte,
	recvChan <-chan *ws.ProtoMsg,
	errChan chan<- error,
	conn net.Conn,
	sessionID, connectionID string,
	tracker *halfCloseTracker,
) {
	var credits chan struct{}
	const v3AckBatch = 4
	const v3AckDelay = 30 * time.Millisecond
	pendingAcks := 0
	var ackTicker *time.Ticker
	var ackTick <-chan time.Time
	if p.proto == protoTypePortForwardV3 {
		ackTicker = time.NewTicker(v3AckDelay)
		ackTick = ackTicker.C
		defer ackTicker.Stop()
	}

	sendAck := func(count int) {
		if count <= 0 {
			return
		}
		var body []byte
		if p.proto == protoTypePortForwardV3 {
			body = make([]byte, 4)
			binary.BigEndian.PutUint32(body, uint32(count))
		}
		m := &ws.ProtoMsg{
			Header: ws.ProtoHdr{
				Proto:     p.proto,
				MsgType:   wspf.MessageTypePortForwardAck,
				SessionID: sessionID,
				Properties: map[string]interface{}{
					wspf.PropertyConnectionID: connectionID,
				},
			},
			Body: body,
		}
		sendForwardMessage(ctx, msgChan, m)
	}
	flushAcks := func() {
		if pendingAcks == 0 {
			return
		}
		sendAck(pendingAcks)
		pendingAcks = 0
	}

	for {
		select {
		case <-ackTick:
			flushAcks()
		case m := <-recvChan:
			switch m.Header.MsgType {
			case wspf.MessageTypePortForwardNew:
				if credits != nil {
					log.Err("duplicate new response received, discarding")
					continue
				}
				readSize, window := tcpFlowLimits(p.proto, m.Body)
				credits = make(chan struct{}, window)
				for i := 0; i < window; i++ {
					credits <- struct{}{}
				}
				go p.handleRequestConnection(ctx, dataChan, errChan, conn, readSize, credits)

			case wspf.MessageTypeError:
				// The device rejected the new request. It owns no socket to stop.
				tracker.markStopped()
				return
			case wspf.MessageTypePortForwardStop:
				// A stop is the peer's EOF. Flush any body first, then close
				// the local side so the application sees EOF instead of a
				// silently stalled tunnel.
				if len(m.Body) != 0 {
					_, _ = conn.Write(m.Body)
				}
				tracker.markStopped()
				_ = conn.Close()
				return

			case wspf.MessageTypePortForward:
				_, err := conn.Write(m.Body)
				if err != nil {
					if errors.Unwrap(err) != net.ErrClosed {
						fmt.Fprintf(os.Stderr, "error: %v\n", err.Error())
					}
				} else if p.proto == ws.ProtoTypePortForward ||
					isFlowControlledPortForward(p.proto) {
					if p.proto == protoTypePortForwardV3 {
						pendingAcks++
						if pendingAcks >= v3AckBatch {
							flushAcks()
						}
					} else {
						sendAck(1)
					}
				}

			case wspf.MessageTypePortForwardAck:
				// v3 batches input-frame ACKs; the body carries the number
				// of credits released. v1/v2 use one credit.
				count := 1
				if p.proto == protoTypePortForwardV3 && len(m.Body) >= 4 {
					count = int(min(binary.BigEndian.Uint32(m.Body[:4]), uint32(portForwardV3TCPWindow)))
				}
				for i := 0; i < min(count, cap(credits)); i++ {
					select {
					case credits <- struct{}{}:
					default:
					}
				}

			case "half_close":
				if p.proto != protoTypePortForwardV3 {
					continue
				}
				flushAcks()
				if tcpConn, ok := conn.(*net.TCPConn); ok {
					if err := tcpConn.CloseWrite(); err != nil {
						fmt.Fprintf(os.Stderr, "error: half-close: %v\n", err)
						return
					}
				} else {
					return
				}
				tracker.markRemote()

			default:
				fmt.Fprintf(os.Stderr, "error: unrecognized message type %s\n",
					m.Header.MsgType)
			}
		case <-ctx.Done():
			return
		}
	}
}

func (p *TCPPortForwarder) sendHalfClose(
	ctx context.Context,
	msgChan chan<- *ws.ProtoMsg,
	sessionID, connectionID string,
) {
	m := &ws.ProtoMsg{
		Header: ws.ProtoHdr{
			Proto:     p.proto,
			MsgType:   "half_close",
			SessionID: sessionID,
			Properties: map[string]interface{}{
				wspf.PropertyConnectionID: connectionID,
			},
		},
	}
	sendForwardMessage(ctx, msgChan, m)
}

func (p *TCPPortForwarder) handleRequest(
	ctx context.Context,
	conn net.Conn,
	sessionID string,
	connectionID string,
	recvChan chan *ws.ProtoMsg,
	msgChan chan *ws.ProtoMsg,
) {
	defer conn.Close()
	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	tracker := newHalfCloseTracker()

	errChan := make(chan error)
	dataChan := make(chan []byte)

	protocol := portforward.PortForwardProtocol(wspf.PortForwardProtocolTCP)
	portforwardNew := &portForwardNewBody{
		Protocol:    &protocol,
		RemoteHost:  &p.remoteHost,
		RemotePort:  &p.remotePort,
		FlowControl: isFlowControlledPortForward(p.proto),
	}
	if p.proto == protoTypePortForwardV3 {
		portforwardNew.TCPFrameSize = portForwardV3FrameSize
		portforwardNew.TCPMaxFrameSize = portForwardV3MaxFrameSize
	}
	body, err := msgpack.Marshal(portforwardNew)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err.Error())
		panic(err)
	}
	m := &ws.ProtoMsg{
		Header: ws.ProtoHdr{
			Proto:     p.proto,
			MsgType:   wspf.MessageTypePortForwardNew,
			SessionID: sessionID,
			Properties: map[string]interface{}{
				wspf.PropertyConnectionID: connectionID,
			},
		},
		Body: body,
	}
	sendForwardMessage(ctx, msgChan, m)

	defer func() {
		conn.Close()
		// The peer retires the flow after a stop or both v3 half-closes.
		select {
		case <-tracker.done:
			return
		default:
		}
		m := &ws.ProtoMsg{
			Header: ws.ProtoHdr{
				Proto:     p.proto,
				MsgType:   wspf.MessageTypePortForwardStop,
				SessionID: sessionID,
				Properties: map[string]interface{}{
					wspf.PropertyConnectionID: connectionID,
				},
			},
		}
		sendForwardMessage(ctx, msgChan, m)
	}()

	go p.handleDeviceMessages(
		connCtx, msgChan, dataChan,
		recvChan, errChan, conn,
		sessionID, connectionID, tracker,
	)

	// go routine to handle sent messages
	for {
		select {
		case err := <-errChan:
			if errors.Is(err, errLocalHalfClose) && p.proto == protoTypePortForwardV3 {
				p.sendHalfClose(connCtx, msgChan, sessionID, connectionID)
				tracker.markLocal()
				continue
			}
			if err != io.EOF {
				fmt.Fprintf(os.Stderr, "error: %v\n", err.Error())
			}
			return
		case data := <-dataChan:

			m := &ws.ProtoMsg{
				Header: ws.ProtoHdr{
					Proto:     p.proto,
					MsgType:   wspf.MessageTypePortForward,
					SessionID: sessionID,
					Properties: map[string]interface{}{
						wspf.PropertyConnectionID: connectionID,
					},
				},
				Body: data,
			}
			sendForwardMessage(ctx, msgChan, m)
		case <-tracker.done:
			return
		case <-ctx.Done():
			return
		}
	}
}

func (p *TCPPortForwarder) handleRequestConnection(
	ctx context.Context,
	dataChan chan<- []byte,
	errChan chan<- error,
	conn net.Conn,
	readLength int,
	credits <-chan struct{},
) {
	data := make([]byte, readLength)

	for {
		n, err := conn.Read(data)
		if err != nil {
			if p.proto == protoTypePortForwardV3 && errors.Is(err, io.EOF) {
				err = errLocalHalfClose
			}
			select {
			case errChan <- err:
			case <-ctx.Done():
			}
			return
		}
		if n > 0 {
			select {
			case <-credits:
			case <-ctx.Done():
				return
			}
			tmp := make([]byte, n)
			copy(tmp, data[:n])
			select {
			case dataChan <- tmp:
			case <-ctx.Done():
				return
			}
		}
	}
}
