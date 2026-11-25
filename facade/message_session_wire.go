package cherryFacade

import (
	"errors"
	"unicode/utf8"

	cproto "github.com/cherry-game/cherry/net/proto"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	clusterSessionField protowire.Number = 6
	sessionDataField    protowire.Number = 7
)

var sessionWireLayoutMatches = func() bool {
	packet := (&cproto.ClusterPacket{}).ProtoReflect().Descriptor().Fields()
	session := (&cproto.Session{}).ProtoReflect().Descriptor().Fields()
	packetSession, sessionData := packet.ByName("session"), session.ByName("data")
	return packetSession != nil && packetSession.Number() == clusterSessionField &&
		session.Len() == 7 && sessionData != nil && sessionData.Number() == sessionDataField &&
		sessionData.IsMap() && sessionData.MapKey().Kind() == protoreflect.StringKind &&
		sessionData.MapValue().Kind() == protoreflect.StringKind
}()

var errInvalidSessionDataUTF8 = errors.New("session data contains invalid UTF-8")

// marshalClusterPacket keeps the ordinary protobuf serializer for the scalar
// fields and encodes the string map without reflection or per-entry allocations.
func marshalClusterPacket(packet *cproto.ClusterPacket) ([]byte, error) {
	session := packet.GetSession()
	data := session.GetData()
	if len(data) == 0 || !sessionWireLayoutMatches || len(session.ProtoReflect().GetUnknown()) != 0 {
		return proto.Marshal(packet)
	}

	header := &cproto.Session{
		Sid: session.Sid, Uid: session.Uid, AgentPath: session.AgentPath, Ip: session.Ip,
		Mid: session.Mid, QueueDeadlineMs: session.QueueDeadlineMs,
	}
	sessionSize := proto.Size(header)
	for key, value := range data {
		if !utf8.ValidString(key) || !utf8.ValidString(value) {
			return nil, errInvalidSessionDataUTF8
		}
		size := 2 + protowire.SizeBytes(len(key)) + protowire.SizeBytes(len(value))
		sessionSize += protowire.SizeTag(sessionDataField) + protowire.SizeBytes(size)
	}

	// Message.Marshal owns this pooled packet exclusively. Its per-request Session
	// remains intact, including the original map, throughout serialization.
	packet.Session = nil
	totalSize := proto.Size(packet) + protowire.SizeTag(clusterSessionField) + protowire.SizeBytes(sessionSize)
	opts := proto.MarshalOptions{}
	wire, err := opts.MarshalAppend(make([]byte, 0, totalSize), packet)
	packet.Session = session
	if err != nil {
		return nil, err
	}
	wire = protowire.AppendTag(wire, clusterSessionField, protowire.BytesType)
	wire = protowire.AppendVarint(wire, uint64(sessionSize))
	wire, err = opts.MarshalAppend(wire, header)
	if err != nil {
		return nil, err
	}
	for key, value := range data {
		size := 2 + protowire.SizeBytes(len(key)) + protowire.SizeBytes(len(value))
		wire = protowire.AppendTag(wire, sessionDataField, protowire.BytesType)
		wire = protowire.AppendVarint(wire, uint64(size))
		wire = protowire.AppendTag(wire, 1, protowire.BytesType)
		wire = protowire.AppendString(wire, key)
		wire = protowire.AppendTag(wire, 2, protowire.BytesType)
		wire = protowire.AppendString(wire, value)
	}
	return wire, nil
}
