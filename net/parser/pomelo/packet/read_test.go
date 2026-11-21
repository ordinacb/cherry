package pomeloPacket

import (
	"bytes"
	"errors"
	"net"
	"testing"

	cerr "github.com/cherry-game/cherry/error"
)

type readPacketConn struct {
	net.Conn
	reader bytes.Reader
	chunk  int
}

func (c *readPacketConn) Read(p []byte) (int, error) {
	if c.chunk > 0 && len(p) > c.chunk {
		p = p[:c.chunk]
	}
	return c.reader.Read(p)
}

func TestReadKeepsPacketBoundariesWithFragmentedStream(t *testing.T) {
	conn := &readPacketConn{chunk: 2}
	body := bytes.Repeat([]byte("payload"), 1024)
	wire := []byte{Heartbeat, 0, 0, 0}
	frame, err := Encode(Data, body)
	if err != nil {
		t.Fatal(err)
	}
	conn.reader.Reset(append(wire, frame...))
	packets, closed, err := Read(conn)
	if err != nil || closed || len(packets) != 1 || packets[0].Type() != Heartbeat || packets[0].Len() != 0 {
		t.Fatalf("read heartbeat: packets=%v closed=%v err=%v", packets, closed, err)
	}
	packets, closed, err = Read(conn)
	if err != nil || closed || len(packets) != 1 || packets[0].Type() != Data || !bytes.Equal(packets[0].Data(), body) {
		t.Fatalf("read fragmented data: packets=%d closed=%v err=%v", len(packets), closed, err)
	}
}

func TestReadPreservesErrorsForIncompletePackets(t *testing.T) {
	cases := []struct {
		wire []byte
		want error
	}{
		{nil, cerr.PacketConnectClosed},
		{[]byte{Data, 0}, cerr.PacketInvalidHeader},
		{[]byte{Data, 0, 0, 3, 7}, cerr.PacketMsgSmallerThanExpected},
		{[]byte{Data, 0, 4, 0, 7}, cerr.PacketMsgSmallerThanExpected},
		{[]byte{None, 0, 0, 0}, cerr.PacketWrongType},
	}
	for _, tc := range cases {
		var conn readPacketConn
		conn.reader.Reset(tc.wire)
		packets, closed, err := Read(&conn)
		if !closed || len(packets) != 0 || !errors.Is(err, tc.want) {
			t.Errorf("read %v: packets=%v closed=%v err=%v, want %v", tc.wire, packets, closed, err, tc.want)
		}
	}
}

func TestReadSmallPacketAllocations(t *testing.T) {
	var conn readPacketConn
	wire := []byte{Data, 0, 0, 1, 7}
	allocations := testing.AllocsPerRun(100, func() {
		conn.reader.Reset(wire)
		_, _, _ = Read(&conn)
	})
	if allocations > 4 {
		t.Errorf("Read allocates %.0f times per small packet, want at most 4", allocations)
	}
}

func BenchmarkReadSmallPacket(b *testing.B) {
	var conn readPacketConn
	wire, err := Encode(Data, bytes.Repeat([]byte{7}, 128))
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		conn.reader.Reset(wire)
		_, _, _ = Read(&conn)
	}
}
