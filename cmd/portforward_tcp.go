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
	// v2 TCP frames are larger than the v1 4 KiB chunk. The Rust daemon
	// accepts the matching limit for an opted-in v2 connection.
	portForwardTCPReadBuffer = 7680
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

func (t *halfCloseTracker) isStopped() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.stopped
}

type TCPPortForwarder struct {
	listen     net.Listener
	remoteHost string
	remotePort uint16
	ackCredits map[string]chan struct{}
	ackMu      sync.RWMutex
	proto      ws.ProtoType
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
		ackCredits: map[string]chan struct{}{},
		proto:      proto,
	}, nil
}

func (p *TCPPortForwarder) Run(
	ctx context.Context,
	sessionID string,
	msgChan chan *ws.ProtoMsg,
	recvChans map[string]chan *ws.ProtoMsg,
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
			acceptedConnections <- conn
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
			recvMu.Lock()
			recvChans[connectionID] = recvChan
			recvMu.Unlock()
			go p.handleRequest(ctx, conn, sessionID, connectionID, recvChan, msgChan)
		case <-ctx.Done():
			return
		}
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
	started := false
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
		msgChan <- m
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
				if started {
					log.Err("duplicate new response received, discarding")
					continue
				}
				// go routine to handle the network connection
				go p.handleRequestConnection(dataChan, errChan, conn)
				started = true

			case wspf.MessageTypePortForwardStop:
				flushAcks()
				tracker.markStopped()
				// A stop is the peer's EOF. Flush any body first, then close
				// the local side so the application sees EOF instead of a
				// silently stalled tunnel.
				if len(m.Body) != 0 {
					_, _ = conn.Write(m.Body)
				}
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
					count = int(binary.BigEndian.Uint32(m.Body[:4]))
				}
				p.ackMu.RLock()
				credits, ok := p.ackCredits[connectionID]
				p.ackMu.RUnlock()
				if ok {
					for i := 0; i < count; i++ {
						select {
						case credits <- struct{}{}:
						default:
						}
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
			flushAcks()
			return
		}
	}
}

func (p *TCPPortForwarder) sendHalfClose(
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
	msgChan <- m
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

	window := 1
	if isFlowControlledPortForward(p.proto) {
		window = portForwardTCPWindow
	}
	credits := make(chan struct{}, window)
	for i := 0; i < window; i++ {
		credits <- struct{}{}
	}
	p.ackMu.Lock()
	p.ackCredits[connectionID] = credits
	p.ackMu.Unlock()
	defer func() {
		p.ackMu.Lock()
		delete(p.ackCredits, connectionID)
		p.ackMu.Unlock()
	}()

	errChan := make(chan error)
	dataChan := make(chan []byte)

	protocol := portforward.PortForwardProtocol(wspf.PortForwardProtocolTCP)
	portforwardNew := &portForwardNewBody{
		Protocol:    &protocol,
		RemoteHost:  &p.remoteHost,
		RemotePort:  &p.remotePort,
		FlowControl: isFlowControlledPortForward(p.proto),
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
	msgChan <- m

	sendStopMessage := true
	defer func() {
		conn.Close()
		if sendStopMessage {
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
			msgChan <- m
		}
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
				p.sendHalfClose(msgChan, sessionID, connectionID)
				tracker.markLocal()
				continue
			}
			if err != io.EOF {
				fmt.Fprintf(os.Stderr, "error: %v\n", err.Error())
			}
			return
		case data := <-dataChan:
			p.ackMu.RLock()
			credits, ok := p.ackCredits[connectionID]
			p.ackMu.RUnlock()
			if ok {
				// Acquire a bounded window slot. v1 uses one slot; v2
				// uses a wider window so the WAN round trip is not paid
				// for every 4 KiB frame.
				<-credits
			}

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
			msgChan <- m
		case <-tracker.done:
			if tracker.isStopped() {
				sendStopMessage = false
			}
			return
		case <-ctx.Done():
			return
		}
	}
}

func (p *TCPPortForwarder) handleRequestConnection(
	dataChan chan<- []byte,
	errChan chan<- error,
	conn net.Conn,
) {
	readLength := readBuffLength
	if isFlowControlledPortForward(p.proto) {
		readLength = portForwardTCPReadBuffer
	}
	data := make([]byte, readLength)

	for {
		n, err := conn.Read(data)
		if err != nil {
			if p.proto == protoTypePortForwardV3 && errors.Is(err, io.EOF) {
				errChan <- errLocalHalfClose
			} else {
				errChan <- err
			}
			break
		}
		if n > 0 {
			tmp := make([]byte, n)
			copy(tmp, data[:n])
			dataChan <- tmp
		}
	}
}
