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
	"encoding/binary"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"sync"

	"github.com/google/uuid"
	"github.com/mendersoftware/go-lib-micro/ws"
	"github.com/mendersoftware/go-lib-micro/ws/portforward"
	wspf "github.com/mendersoftware/go-lib-micro/ws/portforward"
	"github.com/vmihailenco/msgpack"
)

const (
	portForwardUDPChannelSize = 20
	portForwardUDPWindow      = 3
)

type UDPPortForwarder struct {
	conn       *net.UDPConn
	remoteHost string
	remotePort uint16
	sourceAddr *net.UDPAddr
	sourceMu   sync.RWMutex
	ackCredits chan struct{}
	proto      ws.ProtoType
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
	// A local UDP sender can burst far faster than the WebSocket credit
	// window drains. Keep the kernel receive queue from truncating an
	// otherwise deliverable burst.
	_ = conn.SetReadBuffer(4 * 1024 * 1024)
	_ = conn.SetWriteBuffer(4 * 1024 * 1024)
	return &UDPPortForwarder{
		proto:      proto,
		conn:       conn,
		remoteHost: remoteHost,
		remotePort: remotePort,
	}, nil
}

func (p *UDPPortForwarder) Run(
	ctx context.Context,
	sessionID string,
	msgChan chan *ws.ProtoMsg,
	recvChans map[string]chan *ws.ProtoMsg,
) {
	// listen for new connections
	defer p.conn.Close()

	connectionUUID, _ := uuid.NewUUID()
	connectionID := connectionUUID.String()
	if isFlowControlledPortForward(p.proto) {
		connectionID += ":flow"
	}
	recvChan := make(chan *ws.ProtoMsg, portForwardUDPChannelSize)
	recvChans[connectionID] = recvChan
	window := 1
	if isFlowControlledPortForward(p.proto) {
		window = portForwardUDPWindow
	}
	p.ackCredits = make(chan struct{}, window)
	for i := 0; i < window; i++ {
		p.ackCredits <- struct{}{}
	}

	protocol := portforward.PortForwardProtocol(wspf.PortForwardProtocolUDP)
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

	errChan := make(chan error)
	dataChan := make(chan []byte)

	// go routine to handle the network connection
	go p.handleRequestConnection(dataChan, errChan)

	// go routine to handle received messages
	go func() {
		for {
			select {
			case m := <-recvChan:
				if m.Header.Proto == p.proto &&
					m.Header.MsgType == wspf.MessageTypePortForwardStop {
					sendStopMessage = false
					return
				} else if m.Header.Proto == p.proto &&
					m.Header.MsgType == wspf.MessageTypePortForward {
					p.sourceMu.RLock()
					sourceAddr := p.sourceAddr
					p.sourceMu.RUnlock()
					if sourceAddr == nil {
						continue
					}
					_, err := p.conn.WriteToUDP(m.Body, sourceAddr)
					if err != nil {
						fmt.Fprintf(os.Stderr, "error: %v\n", err.Error())
					} else if p.proto == ws.ProtoTypePortForward ||
						p.proto == ws.ProtoTypePortForwardV2 {
						// send the ack (v1 and flow-controlled v2)
						m := &ws.ProtoMsg{
							Header: ws.ProtoHdr{
								Proto:     p.proto,
								MsgType:   wspf.MessageTypePortForwardAck,
								SessionID: sessionID,
								Properties: map[string]interface{}{
									wspf.PropertyConnectionID: connectionID,
								},
							},
						}
						msgChan <- m
					}
				} else if (m.Header.Proto == ws.ProtoTypePortForward ||
					m.Header.Proto == ws.ProtoTypePortForwardV2 ||
					m.Header.Proto == protoTypePortForwardV3) &&
					m.Header.MsgType == wspf.MessageTypePortForwardAck {
					count := 1
					if m.Header.Proto == protoTypePortForwardV3 && len(m.Body) >= 4 {
						count = int(binary.BigEndian.Uint32(m.Body[:4]))
					}
					for i := 0; i < count; i++ {
						select {
						case p.ackCredits <- struct{}{}:
						default:
						}
					}
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// go routine to handle sent messages
	for {
		select {
		case err := <-errChan:
			if err != io.EOF {
				fmt.Fprintf(os.Stderr, "error: %v\n", err.Error())
			}
			return
		case data := <-dataChan:
			if p.proto == ws.ProtoTypePortForward ||
				isFlowControlledPortForward(p.proto) {
				// Acquire a bounded window slot. v1 uses one slot;
				// v2 can keep up to 3 datagrams in flight.
				<-p.ackCredits
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
		case <-ctx.Done():
			return
		}
	}
}

func (p *UDPPortForwarder) handleRequestConnection(dataChan chan []byte, errChan chan error) {
	data := make([]byte, readBuffLength)

	for {
		n, udpAddr, err := p.conn.ReadFromUDP(data[:])
		if err != nil {
			errChan <- err
			break
		}
		if n > 0 {
			p.sourceMu.Lock()
			p.sourceAddr = udpAddr
			p.sourceMu.Unlock()
			tmp := make([]byte, n)
			copy(tmp, data[:n])
			dataChan <- tmp
		}
	}
}
