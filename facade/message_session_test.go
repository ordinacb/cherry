package cherryFacade

import (
	"strconv"
	"strings"
	"testing"

	cproto "github.com/cherry-game/cherry/net/proto"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func TestMarshalSessionDataDoesNotAllocatePerEntry(t *testing.T) {
	message := &Message{Session: &cproto.Session{Data: map[string]string{"one": "value"}}}
	allocs := func() float64 {
		return testing.AllocsPerRun(50, func() {
			if _, err := message.Marshal(); err != nil {
				panic(err)
			}
		})
	}
	small := allocs()
	for i := range 18 {
		message.Session.Data[strconv.Itoa(i)] = "value"
	}
	large := allocs()
	if large > small+1 {
		t.Fatalf("allocations grow with the session map: one entry=%v, nineteen=%v", small, large)
	}
	t.Logf("session map allocations: one entry=%v, nineteen=%v", small, large)
}

func TestMarshalSessionDataMatchesStandardProtobuf(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		for _, malformed := range []bool{false, true} {
			session := &cproto.Session{
				Sid: "sid", Uid: 10001, AgentPath: "gate0.user-out-0", Ip: "::1",
				Mid: 343, QueueDeadlineMs: 123456789,
				Data: map[string]string{"table": strings.Repeat("桌子", 64), "other": "", "": "empty key"},
			}
			if unknown {
				state := session.ProtoReflect()
				state.SetUnknown(protowire.AppendVarint(protowire.AppendTag(nil, 20, protowire.VarintType), 123))
			}
			if malformed {
				session.Data["invalid"] = "\xff"
			}
			message := &Message{
				BuildTime: 155, Source: "gate0.user", Target: "account0.player.10001",
				FuncName: "bag.getBag", Args: []byte{1, 2, 3}, Session: session, RPCDeadlineMs: 342,
			}
			expected := &cproto.ClusterPacket{
				BuildTime: message.BuildTime, SourcePath: message.Source, TargetPath: message.Target,
				FuncName: message.FuncName, ArgBytes: message.Args.([]byte), Session: session, RpcDeadlineMs: message.RPCDeadlineMs,
			}
			_, standardErr := proto.Marshal(expected)
			encoded, err := message.Marshal()
			if (err != nil) != (standardErr != nil) || (err != nil) != malformed {
				t.Fatalf("unknown=%t malformed=%t: marshal error=%v, standard=%v", unknown, malformed, err, standardErr)
			}
			if err != nil {
				continue
			}
			var decoded cproto.ClusterPacket
			if err := proto.Unmarshal(encoded, &decoded); err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(&decoded, expected) {
				t.Fatalf("encoded session changed: got %v, want %v", &decoded, expected)
			}
		}
	}
}
