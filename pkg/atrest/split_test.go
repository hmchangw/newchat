package atrest

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model/cassandra"
)

func TestSplitMessage_AllFields(t *testing.T) {
	in := &cassandra.Message{
		RoomID:      "r1",
		MessageID:   "m1",
		Msg:         "hello",
		Attachments: [][]byte{[]byte("a")},
		Card:        &cassandra.Card{Template: "t", Data: []byte("c")},
		SysMsgData:  []byte("sys"),
		QuotedParentMessage: &cassandra.QuotedParentMessage{
			MessageID:   "p",
			Msg:         "parent body",
			Attachments: [][]byte{[]byte("pa")},
		},
	}
	enc := SplitForEncryption(in)
	assert.Equal(t, "hello", enc.Msg)
	assert.Equal(t, [][]byte{[]byte("a")}, enc.Attachments)
	assert.Equal(t, "t", enc.Card.Template)
	assert.NotNil(t, enc.QuotedParentContent)
	assert.Equal(t, "parent body", enc.QuotedParentContent.Msg)

	// SplitForEncryption MUST NOT mutate the input.
	assert.Equal(t, "hello", in.Msg)
	assert.NotNil(t, in.QuotedParentMessage)
	assert.Equal(t, "parent body", in.QuotedParentMessage.Msg)
}

func TestApplyDecryptedFields_Restores(t *testing.T) {
	target := &cassandra.Message{
		RoomID:    "r1",
		MessageID: "m1",
		QuotedParentMessage: &cassandra.QuotedParentMessage{
			MessageID: "p", // metadata preserved by reader
		},
	}
	// sys_msg_data is set on the row (plaintext column) and must be left
	// untouched by ApplyDecryptedFields since it is not part of the bundle.
	target.SysMsgData = []byte("sys")
	enc := &EncryptedFields{
		Msg:         "hello",
		Attachments: [][]byte{[]byte("a")},
		Card:        &cassandra.Card{Template: "t", Data: []byte("c")},
		QuotedParentContent: &QuotedParentEncrypted{
			Msg:         "parent body",
			Attachments: [][]byte{[]byte("pa")},
		},
	}
	ApplyDecryptedFields(target, enc)
	assert.Equal(t, "hello", target.Msg)
	assert.Equal(t, "t", target.Card.Template)
	assert.Equal(t, []byte("sys"), target.SysMsgData)
	assert.Equal(t, "parent body", target.QuotedParentMessage.Msg)
	assert.Equal(t, [][]byte{[]byte("pa")}, target.QuotedParentMessage.Attachments)
	// metadata preserved
	assert.Equal(t, "p", target.QuotedParentMessage.MessageID)
}

func TestStripEncryptedFields_NullsContent(t *testing.T) {
	in := &cassandra.Message{
		Msg:         "hello",
		Attachments: [][]byte{[]byte("a")},
		Card:        &cassandra.Card{Template: "t"},
		SysMsgData:  []byte("sys"),
		QuotedParentMessage: &cassandra.QuotedParentMessage{
			MessageID:   "p",
			Msg:         "parent body",
			Attachments: [][]byte{[]byte("pa")},
		},
	}
	StripEncryptedFields(in)
	assert.Empty(t, in.Msg)
	assert.Nil(t, in.Attachments)
	assert.Nil(t, in.Card)
	// sys_msg_data is not encrypted, so Strip leaves it on the plaintext column.
	assert.Equal(t, []byte("sys"), in.SysMsgData)
	// quoted_parent_message kept, body fields nulled
	assert.Equal(t, "p", in.QuotedParentMessage.MessageID)
	assert.Empty(t, in.QuotedParentMessage.Msg)
	assert.Nil(t, in.QuotedParentMessage.Attachments)
}

func TestSplitForEncryption_ForwardedContent(t *testing.T) {
	msg := &cassandra.Message{
		RoomID: "r1", MessageID: "m1", Msg: "comment",
		ForwardedMessage: &cassandra.ForwardedMessage{
			MessageID: "m-src", RoomID: "r-src", Msg: "forwarded body",
			Sender: cassandra.Participant{ID: "u5", Account: "eve"},
		},
	}
	out := SplitForEncryption(msg)
	require.NotNil(t, out.ForwardedContent)
	assert.Equal(t, "forwarded body", out.ForwardedContent.Msg)
	// input not mutated
	assert.Equal(t, "forwarded body", msg.ForwardedMessage.Msg)
}

func TestSplitForEncryption_ForwardedContent_EmptyBodyOmitted(t *testing.T) {
	msg := &cassandra.Message{RoomID: "r1", MessageID: "m1",
		ForwardedMessage: &cassandra.ForwardedMessage{MessageID: "m-src", RoomID: "r-src"}}
	assert.Nil(t, SplitForEncryption(msg).ForwardedContent)
}

func TestStripEncryptedFields_BlanksForwardedBody(t *testing.T) {
	msg := &cassandra.Message{Msg: "comment",
		ForwardedMessage: &cassandra.ForwardedMessage{MessageID: "m-src", Msg: "forwarded body"}}
	StripEncryptedFields(msg)
	assert.Empty(t, msg.ForwardedMessage.Msg)
	assert.Equal(t, "m-src", msg.ForwardedMessage.MessageID, "metadata survives")
}

func TestApplyDecryptedFields_RestoresForwardedBody(t *testing.T) {
	msg := &cassandra.Message{ForwardedMessage: &cassandra.ForwardedMessage{MessageID: "m-src"}}
	ApplyDecryptedFields(msg, &EncryptedFields{Msg: "comment", ForwardedContent: &ForwardedEncrypted{Msg: "forwarded body"}})
	assert.Equal(t, "comment", msg.Msg)
	assert.Equal(t, "forwarded body", msg.ForwardedMessage.Msg)

	// Defensive: bundle carries forward content but the UDT column was lost — struct is created.
	bare := &cassandra.Message{}
	ApplyDecryptedFields(bare, &EncryptedFields{ForwardedContent: &ForwardedEncrypted{Msg: "x"}})
	require.NotNil(t, bare.ForwardedMessage)
	assert.Equal(t, "x", bare.ForwardedMessage.Msg)
}
