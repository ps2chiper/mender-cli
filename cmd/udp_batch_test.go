package cmd

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	"github.com/mendersoftware/go-lib-micro/ws"
	wspf "github.com/mendersoftware/go-lib-micro/ws/portforward"
	"github.com/vmihailenco/msgpack"
)

func TestV3UDPFlowRequiresBatchConfirmation(t *testing.T) {
	for _, confirmed := range []bool{false, true} {
		name := "missing-capability"
		if confirmed {
			name = "confirmed"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			cmd := &PortForwardCmd{recvChans: make(map[string]forwardInbox)}
			out := make(chan *ws.ProtoMsg, 8)
			p := &UDPPortForwarder{
				flowProto: protoTypePortForwardV3, flows: make(map[string]*udpFlow),
				msgChan: out, recvChans: cmd.recvChans, recvMu: &cmd.recvMu,
			}
			flow := p.flowFor(ctx, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9123})
			open := nextForwardFrame(t, out, "new")
			var offer portForwardNewBody
			if err := msgpack.Unmarshal(open.Body, &offer); err != nil || !offer.UDPBatch || !offer.FlowControl {
				t.Fatalf("invalid offer: %+v, %v", offer, err)
			}
			flow.queuedBytes.Add(1)
			flow.sendChan <- []byte{42}
			reply := map[string]bool{}
			if confirmed {
				reply["udp_batch_v1"] = true
			}
			body, err := msgpack.Marshal(reply)
			if err != nil {
				t.Fatal(err)
			}
			p.recvMu.RLock()
			done := cmd.recvChans[flow.connectionID].done
			p.recvMu.RUnlock()
			cmd.deliver(flow.connectionID, &ws.ProtoMsg{Header: open.Header, Body: body})
			if confirmed {
				frame := nextForwardFrame(t, out, "forward")
				packets, err := splitDatagrams(frame.Body)
				if err != nil || len(packets) != 1 || !bytes.Equal(packets[0], []byte{42}) {
					t.Fatalf("queued datagram: %v, %v", packets, err)
				}
				return
			}
			select {
			case <-done:
			case <-ctx.Done():
				t.Fatal("unconfirmed flow remained open")
			}
			nextForwardFrame(t, out, "stop")
		})
	}
}

func TestUDPBatchWireFormat(t *testing.T) {
	frame := appendDatagram(nil, nil)
	frame = appendDatagram(frame, []byte{42})
	if !bytes.Equal(frame, []byte{0, 0, 0, 0, 0, 1, 42}) {
		t.Fatal(frame)
	}
	pkts, err := splitDatagrams(frame)
	if err != nil || len(pkts) != 2 || len(pkts[0]) != 0 || !bytes.Equal(pkts[1], []byte{42}) {
		t.Fatal(pkts, err)
	}
	for _, bad := range [][]byte{nil, {0}, {0, 1, 42, 0}, {0, 2, 42}, make([]byte, 132)} {
		if _, err = splitDatagrams(bad); err == nil {
			t.Fatalf("accepted malformed batch of %d bytes", len(bad))
		}
	}
}

func TestV3UDPRetiresFlowWhileSessionWriterIsBusy(t *testing.T) {
	for _, scenario := range []string{"data-before-accept", "remote-write-error"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			out := make(chan *ws.ProtoMsg, 1)
			cmd := &PortForwardCmd{recvChans: make(map[string]forwardInbox)}
			p := &UDPPortForwarder{flowProto: protoTypePortForwardV3, flows: make(map[string]*udpFlow),
				msgChan: out, recvChans: cmd.recvChans, recvMu: &cmd.recvMu}
			source := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9123}
			flow := p.flowFor(ctx, source)
			open := nextForwardFrame(t, out, "new")
			inbox := cmd.recvChans[flow.connectionID]
			out <- open // Occupy the session writer until the flow is canceled.
			hdr := open.Header
			if scenario == "data-before-accept" {
				hdr.MsgType = "forward"
				cmd.deliver(flow.connectionID, &ws.ProtoMsg{Header: hdr, Body: appendDatagram(nil, []byte{42})})
			} else {
				cmd.deliver(flow.connectionID, open)
				kind, reason := "forward", "target write failed"
				body, err := msgpack.Marshal(wspf.Error{MessageType: &kind, Error: &reason})
				if err != nil {
					t.Fatal(err)
				}
				hdr.MsgType = "error"
				if err := cmd.handleForwardError(&ws.ProtoMsg{Header: hdr, Body: body}); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case <-inbox.done:
			case <-ctx.Done():
				t.Fatal("failed flow remained active")
			}
			<-out
			stop := nextForwardFrame(t, out, "stop")
			if stop.Header.Properties[wspf.PropertyConnectionID] != flow.connectionID {
				t.Fatal("wrong flow stopped")
			}
			next := p.flowFor(ctx, source)
			if next.connectionID == flow.connectionID {
				t.Fatal("reused a failed flow")
			}
			retry := nextForwardFrame(t, out, "new")
			cmd.deliver(next.connectionID, retry)
			next.queuedBytes.Add(1)
			next.sendChan <- []byte{7}
			frame := nextForwardFrame(t, out, "forward")
			packets, err := splitDatagrams(frame.Body)
			if err != nil || len(packets) != 1 || !bytes.Equal(packets[0], []byte{7}) {
				t.Fatalf("retry: %v, %v", packets, err)
			}
		})
	}
}
