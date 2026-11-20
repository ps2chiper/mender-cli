// Copyright 2021 Northern.tech AS
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
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/mendersoftware/go-lib-micro/ws"
	"github.com/mendersoftware/go-lib-micro/ws/portforward"
	wspf "github.com/mendersoftware/go-lib-micro/ws/portforward"
	"github.com/vmihailenco/msgpack"
)

const (
	portForwardUDPChannelSize = 20
	portForwardUDPQueueSize   = 256
	portForwardUDPWindow      = 3
	portForwardUDPMaxFlows    = 64
	portForwardUDPIdle        = 30 * time.Second
	portForwardUDPSweep       = 5 * time.Second
)

// UDPPortForwarder owns one local UDP listener and creates an independent
// Mender port-forward connection for each observed local UDP source. The
// daemon then gives each source its own connected target socket, so responses
// cannot be misrouted between clients. This is the useful part of chisel's
// per-source UDP flow table, expressed with Mender connection IDs.
type UDPPortForwarder struct {
	conn       *net.UDPConn
	remoteHost string
	remotePort uint16
	proto      ws.ProtoType
	flowProto  ws.ProtoType
	sessionID  string
	msgChan    chan *ws.ProtoMsg
	recvChans  map[string]chan *ws.ProtoMsg
	recvMu     *sync.RWMutex

	mu    sync.Mutex
	flows map[string]*udpFlow
}

type udpFlow struct {
	p            *UDPPortForwarder
	source       *net.UDPAddr
	connectionID string
	recvChan     chan *ws.ProtoMsg
	sendChan     chan []byte
	credits      chan struct{}
	lastSeen     atomic.Int64
	closed       atomic.Bool
}

func NewUDPPortForwarder(
	bindingHost string,
	localPort uint16,
	remoteHost string,
	remotePort uint16,
	proto ws.ProtoType,
) (*UDPPortForwarder, error) {
	fmt.Printf(
		"Forwarding from udp/%s:%d -> udp/%s:%d\n",
		bindingHost,
		localPort,
		remoteHost,
		remotePort,
	)
	sAddr, err := net.ResolveUDPAddr("udp", bindingHost+":"+strconv.Itoa(int(localPort)))
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP(protocolUDP, sAddr)
	if err != nil {
		return nil, err
	}
	_ = conn.SetReadBuffer(4 * 1024 * 1024)
	_ = conn.SetWriteBuffer(4 * 1024 * 1024)
	flowProto := proto
	if flowProto == protoTypePortForwardV3 {
		// v3 UDP source fan-out uses the stable v2 datagram transport; each
		// source has a separate connection and target socket.
		flowProto = ws.ProtoTypePortForwardV2
	}
	return &UDPPortForwarder{
		proto:      proto,
		flowProto:  flowProto,
		conn:       conn,
		remoteHost: remoteHost,
		remotePort: remotePort,
		flows:      make(map[string]*udpFlow),
	}, nil
}

func (p *UDPPortForwarder) Run(
	ctx context.Context,
	sessionID string,
	msgChan chan *ws.ProtoMsg,
	recvChans map[string]chan *ws.ProtoMsg,
	recvMu *sync.RWMutex,
) {
	defer p.conn.Close()
	p.sessionID = sessionID
	p.msgChan = msgChan
	p.recvChans = recvChans
	p.recvMu = recvMu

	go p.cleanup(ctx)
	p.readLoop(ctx)
	p.closeAll()
}

func (p *UDPPortForwarder) readLoop(ctx context.Context) {
	buffer := make([]byte, readBuffLength)
	for {
		_ = p.conn.SetReadDeadline(time.Now().Add(time.Second))
		n, source, err := p.conn.ReadFromUDP(buffer)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				select {
				case <-ctx.Done():
					return
				default:
					continue
				}
			}
			select {
			case <-ctx.Done():
			default:
				fmt.Fprintf(os.Stderr, "error: %v\n", err)
			}
			return
		}
		if n == 0 {
			continue
		}
		flow := p.flowFor(ctx, source)
		if flow == nil {
			continue
		}
		data := make([]byte, n)
		copy(data, buffer[:n])
		select {
		case flow.sendChan <- data:
			flow.lastSeen.Store(time.Now().UnixNano())
		default:
			// This source's bounded queue is full; UDP remains best effort.
		}
	}
}

func (p *UDPPortForwarder) flowFor(ctx context.Context, source *net.UDPAddr) *udpFlow {
	key := source.String()
	p.mu.Lock()
	if flow, ok := p.flows[key]; ok {
		p.mu.Unlock()
		return flow
	}
	if len(p.flows) >= portForwardUDPMaxFlows {
		p.mu.Unlock()
		return nil
	}
	connectionUUID, _ := uuid.NewUUID()
	connectionID := connectionUUID.String()
	if isFlowControlledPortForward(p.flowProto) {
		connectionID += ":flow"
	}
	window := 1
	if isFlowControlledPortForward(p.flowProto) {
		window = portForwardUDPWindow
	}
	flow := &udpFlow{
		p:            p,
		source:       source,
		connectionID: connectionID,
		recvChan:     make(chan *ws.ProtoMsg, portForwardUDPChannelSize),
		sendChan:     make(chan []byte, portForwardUDPQueueSize),
		credits:      make(chan struct{}, window),
	}
	for i := 0; i < window; i++ {
		flow.credits <- struct{}{}
	}
	flow.lastSeen.Store(time.Now().UnixNano())
	p.flows[key] = flow
	p.mu.Unlock()

	p.recvMu.Lock()
	p.recvChans[connectionID] = flow.recvChan
	p.recvMu.Unlock()

	go flow.run(ctx)
	go flow.sendLoop(ctx)
	p.sendNew(flow)
	return flow
}

func (p *UDPPortForwarder) sendNew(flow *udpFlow) {
	protocol := portforward.PortForwardProtocol(wspf.PortForwardProtocolUDP)
	body, err := msgpack.Marshal(&portForwardNewBody{
		Protocol:    &protocol,
		RemoteHost:  &p.remoteHost,
		RemotePort:  &p.remotePort,
		FlowControl: isFlowControlledPortForward(p.flowProto),
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return
	}
	p.msgChan <- &ws.ProtoMsg{
		Header: ws.ProtoHdr{
			Proto:     p.flowProto,
			MsgType:   wspf.MessageTypePortForwardNew,
			SessionID: p.sessionID,
			Properties: map[string]interface{}{
				wspf.PropertyConnectionID: flow.connectionID,
			},
		},
		Body: body,
	}
}

func (f *udpFlow) run(ctx context.Context) {
	defer f.p.removeFlow(f)
	for {
		select {
		case m := <-f.recvChan:
			switch m.Header.MsgType {
			case wspf.MessageTypePortForwardNew:
				continue
			case wspf.MessageTypePortForward:
				if _, err := f.p.conn.WriteToUDP(m.Body, f.source); err != nil {
					fmt.Fprintf(os.Stderr, "error: %v\n", err)
					continue
				}
				f.lastSeen.Store(time.Now().UnixNano())
				if f.p.flowProto == ws.ProtoTypePortForward ||
					isFlowControlledPortForward(f.p.flowProto) {
					f.p.msgChan <- &ws.ProtoMsg{
						Header: ws.ProtoHdr{
							Proto:     f.p.flowProto,
							MsgType:   wspf.MessageTypePortForwardAck,
							SessionID: f.p.sessionID,
							Properties: map[string]interface{}{
								wspf.PropertyConnectionID: f.connectionID,
							},
						},
					}
				}
			case wspf.MessageTypePortForwardAck:
				select {
				case f.credits <- struct{}{}:
				default:
				}
			case wspf.MessageTypePortForwardStop:
				f.close(false)
				return
			case wspf.MessageTypeError:
				fmt.Fprintf(os.Stderr, "port-forward UDP flow error: %s\n", string(m.Body))
				f.close(false)
				return
			default:
				continue
			}
		case <-ctx.Done():
			return
		}
	}
}

func (f *udpFlow) sendLoop(ctx context.Context) {
	for {
		select {
		case data := <-f.sendChan:
			select {
			case <-f.credits:
			case <-ctx.Done():
				return
			}
			if f.closed.Load() {
				continue
			}
			f.lastSeen.Store(time.Now().UnixNano())
			f.p.msgChan <- &ws.ProtoMsg{
				Header: ws.ProtoHdr{
					Proto:     f.p.flowProto,
					MsgType:   wspf.MessageTypePortForward,
					SessionID: f.p.sessionID,
					Properties: map[string]interface{}{
						wspf.PropertyConnectionID: f.connectionID,
					},
				},
				Body: data,
			}
		case <-ctx.Done():
			return
		}
	}
}

func (f *udpFlow) close(sendStop bool) {
	if !f.closed.CompareAndSwap(false, true) {
		return
	}
	f.p.removeFlow(f)
	if sendStop {
		f.p.msgChan <- &ws.ProtoMsg{
			Header: ws.ProtoHdr{
				Proto:     f.p.flowProto,
				MsgType:   wspf.MessageTypePortForwardStop,
				SessionID: f.p.sessionID,
				Properties: map[string]interface{}{
					wspf.PropertyConnectionID: f.connectionID,
				},
			},
		}
	}
}

func (p *UDPPortForwarder) removeFlow(flow *udpFlow) {
	p.recvMu.Lock()
	delete(p.recvChans, flow.connectionID)
	p.recvMu.Unlock()
	p.mu.Lock()
	if p.flows[flow.source.String()] == flow {
		delete(p.flows, flow.source.String())
	}
	p.mu.Unlock()
}

func (p *UDPPortForwarder) cleanup(ctx context.Context) {
	ticker := time.NewTicker(portForwardUDPSweep)
	defer ticker.Stop()
	for {
		select {
		case now := <-ticker.C:
			cutoff := now.Add(-portForwardUDPIdle).UnixNano()
			p.mu.Lock()
			expired := make([]*udpFlow, 0)
			for _, flow := range p.flows {
				if flow.lastSeen.Load() < cutoff {
					expired = append(expired, flow)
				}
			}
			p.mu.Unlock()
			for _, flow := range expired {
				flow.close(true)
			}
		case <-ctx.Done():
			return
		}
	}
}

func (p *UDPPortForwarder) closeAll() {
	p.mu.Lock()
	flows := make([]*udpFlow, 0, len(p.flows))
	for _, flow := range p.flows {
		flows = append(flows, flow)
	}
	p.mu.Unlock()
	for _, flow := range flows {
		flow.close(true)
	}
}
