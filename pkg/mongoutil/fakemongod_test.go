package mongoutil

import (
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
)

// fakeMongod is a minimal wire-protocol server for unit tests that need the
// driver to complete a handshake and then be REFUSED at authentication: it
// answers hello/isMaster as a writable primary and every saslStart with
// AuthenticationFailed (18). A monitor connection never authenticates, so the
// topology reads healthy while every pooled connection fails SCRAM — exactly the
// shape of a real server rejecting our credentials. It runs in-process, so
// these tests are unit tests: no container, no real database.
type fakeMongod struct {
	ln net.Listener
}

func startFakeMongod(t *testing.T) *fakeMongod {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	f := &fakeMongod{ln: ln}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go f.serve(c)
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })
	return f
}

func (f *fakeMongod) uri() string { return "mongodb://" + f.ln.Addr().String() }

const (
	opReply = 1
	opQuery = 2004
	opMsg   = 2013
)

func (f *fakeMongod) respond(cmd bsoncore.Document) bson.D {
	elems, _ := cmd.Elements()
	if len(elems) == 0 {
		return bson.D{{Key: "ok", Value: 1.0}}
	}
	key := elems[0].Key()
	if key == "$query" {
		if sub, ok := elems[0].Value().DocumentOK(); ok {
			return f.respond(sub)
		}
	}
	switch key {
	case "isMaster", "ismaster", "hello":
		return bson.D{
			{Key: "ismaster", Value: true}, {Key: "isWritablePrimary", Value: true}, {Key: "helloOk", Value: true},
			{Key: "maxBsonObjectSize", Value: int32(16777216)}, {Key: "maxMessageSizeBytes", Value: int32(48000000)},
			{Key: "maxWriteBatchSize", Value: int32(100000)}, {Key: "localTime", Value: bson.NewDateTimeFromTime(time.Now())},
			{Key: "logicalSessionTimeoutMinutes", Value: int32(30)}, {Key: "connectionId", Value: int32(1)},
			{Key: "minWireVersion", Value: int32(0)}, {Key: "maxWireVersion", Value: int32(21)},
			{Key: "readOnly", Value: false}, {Key: "ok", Value: 1.0},
		}
	case "saslStart": // answered with a failure, so saslContinue never follows
		return bson.D{{Key: "ok", Value: 0.0}, {Key: "errmsg", Value: "Authentication failed."},
			{Key: "code", Value: int32(18)}, {Key: "codeName", Value: "AuthenticationFailed"}}
	default:
		return bson.D{{Key: "ok", Value: 1.0}}
	}
}

func (f *fakeMongod) serve(conn net.Conn) {
	defer func() { _ = conn.Close() }()
	var nextID uint32 = 1000
	for {
		hdr := make([]byte, 16)
		if _, err := io.ReadFull(conn, hdr); err != nil {
			return
		}
		length := binary.LittleEndian.Uint32(hdr[0:4])
		reqID := binary.LittleEndian.Uint32(hdr[4:8])
		opcode := binary.LittleEndian.Uint32(hdr[12:16])
		if length < 16 || length > maxWireMessage {
			return
		}
		body := make([]byte, length-16)
		if _, err := io.ReadFull(conn, body); err != nil {
			return
		}
		var cmd bsoncore.Document
		switch opcode {
		case opQuery: // flags(4) cstring ns skip(4) limit(4) doc
			p := 4
			for p < len(body) && body[p] != 0 {
				p++
			}
			p += 1 + 4 + 4
			if p > len(body) {
				return
			}
			cmd = bsoncore.Document(body[p:])
		case opMsg: // flags(4) section kind(1) doc
			if len(body) < 5 {
				return
			}
			cmd = bsoncore.Document(body[5:])
		default:
			return
		}
		doc, _ := bson.Marshal(f.respond(cmd))
		nextID++
		var out []byte
		switch opcode {
		case opQuery:
			out = header(out, wireLen(16+4+8+4+4+len(doc)), nextID, reqID, opReply)
			out = binary.LittleEndian.AppendUint32(out, 0) // responseFlags
			out = binary.LittleEndian.AppendUint64(out, 0) // cursorID
			out = binary.LittleEndian.AppendUint32(out, 0) // startingFrom
			out = binary.LittleEndian.AppendUint32(out, 1) // numberReturned
			out = append(out, doc...)
		case opMsg:
			out = header(out, wireLen(16+4+1+len(doc)), nextID, reqID, opMsg)
			out = binary.LittleEndian.AppendUint32(out, 0) // flagBits
			out = append(out, 0)                           // section kind 0
			out = append(out, doc...)
		}
		if _, err := conn.Write(out); err != nil {
			return
		}
	}
}

// maxWireMessage caps a message the fake will read; the replies it builds are
// a few hundred bytes and the driver's requests are smaller still.
const maxWireMessage = 1 << 20

// wireLen narrows a reply length to the header's uint32 field. The replies
// here are tiny, so the check is defensive; a panic beats a truncated frame.
func wireLen(n int) uint32 {
	if n < 0 || n > maxWireMessage {
		panic("fakeMongod: reply length out of range")
	}
	// #nosec G115 -- bounds checked directly above
	return uint32(n)
}

func header(dst []byte, length, reqID, respTo, opcode uint32) []byte {
	dst = binary.LittleEndian.AppendUint32(dst, length)
	dst = binary.LittleEndian.AppendUint32(dst, reqID)
	dst = binary.LittleEndian.AppendUint32(dst, respTo)
	dst = binary.LittleEndian.AppendUint32(dst, opcode)
	return dst
}
