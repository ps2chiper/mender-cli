package cmd

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	"github.com/mendersoftware/go-lib-micro/ws"
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
			select {
			case frame := <-out:
				if frame.Header.MsgType != "stop" {
					t.Fatalf("sent %q on an unconfirmed flow", frame.Header.MsgType)
				}
			default:
			}
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
