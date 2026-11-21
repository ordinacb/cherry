package pomeloPacket

import (
	"bytes"
	"fmt"
	"io"
	"net"

	cerr "github.com/cherry-game/cherry/error"
)

type (
	Packet struct {
		typ  Type
		len  int
		data []byte
	}
)

func (p *Packet) Type() Type {
	return p.typ
}

func (p *Packet) Len() int {
	return p.len
}

func (p *Packet) Data() []byte {
	return p.data
}

func (p *Packet) SetData(data []byte) {
	p.data = data
}

// String represents the Packet's in text mode.
func (p *Packet) String() string {
	return fmt.Sprintf("packet type: %s, length: %d, data: %s", TypeName(p.typ), p.len, string(p.data))
}

func Decode(data []byte) ([]*Packet, error) {
	buf := bytes.NewBuffer(data)

	var (
		packets []*Packet
		err     error
	)

	// check length
	if buf.Len() < HeadLength {
		return nil, err
	}

	size, typ, err := forward(buf)
	if err != nil {
		return nil, err
	}

	for size <= buf.Len() {
		pkg := &Packet{
			typ:  typ,
			len:  size,
			data: buf.Next(size),
		}

		packets = append(packets, pkg)

		if buf.Len() < HeadLength {
			break
		}

		size, typ, err = forward(buf)
		if err != nil {
			return nil, err
		}
	}

	return packets, nil
}

// Encode create a packet.Packet from  the raw bytes slice and then encode to network bytes slice
// Protocol refs: https://github.com/NetEase/pomelo/wiki/Communication-Protocol
//
// -<type>-|--------<length>--------|-<data>-
// --------|------------------------|--------
// 1 byte packet type, 3 bytes packet data length(big end), and data segment
func Encode(typ byte, data []byte) ([]byte, error) {
	if typ < Handshake || typ > Kick {
		return nil, cerr.PacketWrongType
	}

	if len(data) > WriteMaxPacketSize {
		return nil, cerr.PacketSizeExceed
	}

	pkg := &Packet{
		typ: typ,
		len: len(data),
	}

	// header+body = 4 + len(body)
	buf := make([]byte, pkg.len+HeadLength)

	//第一个字节存放消息类型
	buf[0] = pkg.Type()

	//2~4 字节 存放消息长度
	copy(buf[1:HeadLength], IntToBytes(pkg.len))

	//4字节之后存放的内容是消息体
	copy(buf[HeadLength:], data)

	return buf, nil
}

func Read(conn net.Conn) ([]*Packet, bool, error) {
	header := make([]byte, HeadLength)
	if _, err := io.ReadFull(conn, header); err == io.EOF {
		return nil, true, cerr.PacketConnectClosed
	} else if err == io.ErrUnexpectedEOF {
		return nil, true, cerr.PacketInvalidHeader
	} else if err != nil {
		return nil, true, err
	}

	size, err := ParseHeader(header)
	if err != nil {
		return nil, true, err
	}
	data, err := readBody(conn, size)
	if err != nil {
		return nil, true, err
	}
	return []*Packet{{typ: header[0], len: size, data: data}}, false, nil
}

func readBody(conn net.Conn, size int) ([]byte, error) {
	// Allocate small bodies directly. Grow the buffer as data arrives for
	// larger bodies so a partial frame cannot reserve its entire declared size.
	const smallBodyLimit = 512
	if size <= smallBodyLimit {
		data := make([]byte, size)
		_, err := io.ReadFull(conn, data)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, cerr.PacketMsgSmallerThanExpected
		}
		return data, err
	}
	data, err := io.ReadAll(io.LimitReader(conn, int64(size)))
	if err != nil {
		return nil, err
	}
	if len(data) < size {
		return nil, cerr.PacketMsgSmallerThanExpected
	}
	return data, nil
}
