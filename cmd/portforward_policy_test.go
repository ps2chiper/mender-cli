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
	"io"
	"net"
	"testing"
	"time"

	"github.com/mendersoftware/go-lib-micro/ws"
	wspf "github.com/mendersoftware/go-lib-micro/ws/portforward"
	"github.com/vmihailenco/msgpack"
)

func nextForwardFrame(t *testing.T, ch <-chan *ws.ProtoMsg, typ string) *ws.ProtoMsg {
	t.Helper()
	select {
	case m := <-ch:
		if m.Header.MsgType != typ {
			t.Fatalf("got %s, want %s", m.Header.MsgType, typ)
		}
		return m
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", typ)
		return nil
	}
}

func rejectOpen(t *testing.T, cmd *PortForwardCmd, open *ws.ProtoMsg) {
	t.Helper()
	reason, op := "destination denied by device policy", "new"
	body, err := msgpack.Marshal(wspf.Error{Error: &reason, MessageType: &op})
	if err != nil {
		t.Fatal(err)
	}
	header := open.Header
	header.MsgType = wspf.MessageTypeError
	if err := cmd.handleForwardError(&ws.ProtoMsg{Header: header, Body: body}); err != nil {
		t.Fatal(err)
	}
}

func TestTCPDeniedOpenClosesOnlyItsConnection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p, err := NewTCPPortForwarder("127.0.0.1", 0, "127.0.0.1", 443, protoTypePortForwardV3)
	if err != nil {
		t.Fatal(err)
	}
	cmd := &PortForwardCmd{recvChans: make(map[string]forwardInbox)}
	out := make(chan *ws.ProtoMsg, 8)
	go p.Run(ctx, "sid", out, cmd.recvChans, &cmd.recvMu)
	dial := func() (net.Conn, *ws.ProtoMsg) {
		conn, err := net.DialTimeout("tcp", p.listen.Addr().String(), 2*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		if err := conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
			t.Fatal(err)
		}
		return conn, nextForwardFrame(t, out, "new")
	}
	denied, first := dial()
	allowed, second := dial()
	rejectOpen(t, cmd, first)
	var buf [1]byte
	if n, err := denied.Read(buf[:]); n != 0 || err != io.EOF {
		t.Fatalf("denied socket: read %d, %v", n, err)
	}
	id := second.Header.Properties[wspf.PropertyConnectionID].(string)
	cmd.deliver(id, second)
	if _, err := allowed.Write([]byte("still open")); err != nil {
		t.Fatal(err)
	}
	if got := nextForwardFrame(t, out, "forward"); string(got.Body) != "still open" || got.Header.Properties[wspf.PropertyConnectionID] != id {
		t.Fatal("other flow did not deliver its payload")
	}
}

func TestUDPWaitsForAcceptanceAndRetiresDeniedFlow(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cmd := &PortForwardCmd{recvChans: make(map[string]forwardInbox)}
	out := make(chan *ws.ProtoMsg, 8)
	p := &UDPPortForwarder{flowProto: ws.ProtoTypePortForwardV2, flows: make(map[string]*udpFlow),
		msgChan: out, recvChans: cmd.recvChans, recvMu: &cmd.recvMu}
	source := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1234}
	denied := p.flowFor(ctx, source)
	first := nextForwardFrame(t, out, "new")
	denied.sendChan <- []byte("drop")
	select {
	case m := <-out:
		t.Fatalf("unexpected %q before acceptance", m.Header.MsgType)
	case <-time.After(25 * time.Millisecond):
	}
	rejectOpen(t, cmd, first)
	deadline := time.Now().Add(2 * time.Second)
	for {
		p.recvMu.RLock()
		removed := p.flows[source.String()] == nil
		p.recvMu.RUnlock()
		if removed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("denied UDP flow retained")
		}
		time.Sleep(time.Millisecond)
	}
	allowed := p.flowFor(ctx, source)
	second := nextForwardFrame(t, out, "new")
	allowed.sendChan <- []byte("accepted")
	cmd.deliver(allowed.connectionID, second)
	if got := nextForwardFrame(t, out, "forward"); string(got.Body) != "accepted" || got.Header.Properties[wspf.PropertyConnectionID] != allowed.connectionID {
		t.Fatal("unexpected datagram after retry")
	}
}

func TestUDPConcurrentFlowRegistrationAndRetirement(t *testing.T) {
	const sources = 8
	const rounds = 80
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := &PortForwardCmd{recvChans: make(map[string]forwardInbox)}
	p := &UDPPortForwarder{
		flows: make(map[string]*udpFlow), recvChans: cmd.recvChans, recvMu: &cmd.recvMu,
		msgChan: make(chan *ws.ProtoMsg, sources*rounds),
	}
	start := make(chan struct{})
	done := make(chan struct{}, sources)
	for i := 0; i < sources; i++ {
		go func(port int) {
			source := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: port}
			<-start
			for j := 0; j < rounds; j++ {
				flow := p.flowFor(ctx, source)
				p.recvMu.RLock()
				consistent := len(p.flows) == len(p.recvChans)
				for _, active := range p.flows {
					if p.recvChans[active.connectionID].messages != active.recvChan {
						consistent = false
					}
				}
				p.recvMu.RUnlock()
				if !consistent {
					t.Errorf("inconsistent UDP flow and inbox registries during churn")
					break
				}
				flow.close(ctx, false)
			}
			done <- struct{}{}
		}(12000 + i)
	}
	close(start)
	for i := 0; i < sources; i++ {
		select {
		case <-done:
		case <-ctx.Done():
			t.Fatal("concurrent UDP registration and retirement did not finish")
		}
	}
	p.recvMu.RLock()
	defer p.recvMu.RUnlock()
	if len(p.flows) != 0 || len(p.recvChans) != 0 {
		t.Fatalf("retained %d UDP flows and %d inboxes", len(p.flows), len(p.recvChans))
	}
}

func TestTCPGracefulTeardownFrames(t *testing.T) {
	for _, tc := range []struct {
		name        string
		proto       ws.ProtoType
		deviceFirst bool
	}{
		{"v3-local-first", protoTypePortForwardV3, false},
		{"v3-device-first", protoTypePortForwardV3, true},
		{"v2-local-eof", ws.ProtoTypePortForwardV2, false},
		{"v1-local-eof", ws.ProtoTypePortForward, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			p, err := NewTCPPortForwarder("127.0.0.1", 0, "127.0.0.1", 443, tc.proto)
			if err != nil {
				t.Fatal(err)
			}
			defer p.listen.Close()
			app, err := net.DialTCP("tcp", nil, p.listen.Addr().(*net.TCPAddr))
			if err != nil {
				t.Fatal(err)
			}
			defer app.Close()
			if err := app.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
				t.Fatal(err)
			}
			conn, err := p.listen.Accept()
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			in, out := make(chan *ws.ProtoMsg, 4), make(chan *ws.ProtoMsg, 8)
			done := make(chan struct{})
			go func() {
				p.handleRequest(ctx, conn, "sid", "id:flow", in, out)
				close(done)
			}()
			open := nextForwardFrame(t, out, "new")
			in <- open
			deviceEOF := func() {
				header := open.Header
				header.MsgType = "forward"
				in <- &ws.ProtoMsg{Header: header, Body: []byte("tail")}
				header.MsgType = "half_close"
				in <- &ws.ProtoMsg{Header: header}
				tail, err := io.ReadAll(app)
				if err != nil || string(tail) != "tail" {
					t.Fatalf("device tail %q: %v", tail, err)
				}
				nextForwardFrame(t, out, "ack")
			}
			if tc.deviceFirst {
				deviceEOF()
			}
			if err := app.CloseWrite(); err != nil {
				t.Fatal(err)
			}
			if tc.proto == protoTypePortForwardV3 {
				nextForwardFrame(t, out, "half_close")
				if !tc.deviceFirst {
					deviceEOF()
				}
			} else {
				nextForwardFrame(t, out, "stop")
			}
			select {
			case <-done:
			case <-ctx.Done():
				t.Fatal("TCP handler did not finish")
			}
			select {
			case m := <-out:
				t.Fatalf("unexpected %q after teardown", m.Header.MsgType)
			default:
			}
		})
	}
}

func TestTCPFrameNegotiation(t *testing.T) {
	accept, err := msgpack.Marshal(map[string]uint32{"tcp_frame_size": 32768})
	if err != nil {
		t.Fatal(err)
	}
	largeAccept, err := msgpack.Marshal(map[string]uint32{"tcp_frame_size": 262144})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		proto         ws.ProtoType
		reply         []byte
		frame, window int
	}{
		{protoTypePortForwardV3, largeAccept, 262144, 16},
		{protoTypePortForwardV3, accept, 32768, 16},
		{protoTypePortForwardV3, nil, 7680, 16},
		{protoTypePortForwardV3, []byte{0xc1}, 7680, 16},
		{ws.ProtoTypePortForwardV2, accept, 7680, 8},
		{ws.ProtoTypePortForward, accept, 4096, 1},
	} {
		size, window := tcpFlowLimits(tc.proto, tc.reply)
		if size != tc.frame || window != tc.window {
			t.Errorf("protocol %v: got %d/%d, want %d/%d", tc.proto, size, window, tc.frame, tc.window)
		}
	}
}
