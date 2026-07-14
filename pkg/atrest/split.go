package atrest

import "github.com/hmchangw/chat/pkg/model/cassandra"

// SplitForEncryption returns a copy of msg's user-authored fields suitable
// for passing to Cipher.Encrypt. The input is not mutated.
func SplitForEncryption(msg *cassandra.Message) EncryptedFields {
	out := EncryptedFields{
		Msg:         msg.Msg,
		Attachments: msg.Attachments,
		Card:        msg.Card,
		CardAction:  msg.CardAction,
	}
	if msg.QuotedParentMessage != nil {
		q := msg.QuotedParentMessage
		if q.Msg != "" || len(q.Attachments) > 0 {
			out.QuotedParentContent = &QuotedParentEncrypted{
				Msg:         q.Msg,
				Attachments: q.Attachments,
			}
		}
	}
	return out
}

// StripEncryptedFields nulls out user-authored fields on msg in place.
// Call this after SplitForEncryption to produce the metadata-only struct
// that gets written to Cassandra alongside enc_payload. sys_msg_data is left
// intact — it is not encrypted and stays in its plaintext column.
//
// quoted_parent_message metadata (sender, IDs, timestamps) is preserved;
// only its body fields (msg, attachments) are nulled.
func StripEncryptedFields(msg *cassandra.Message) {
	msg.Msg = ""
	msg.Attachments = nil
	msg.Card = nil
	msg.CardAction = nil
	if msg.QuotedParentMessage != nil {
		msg.QuotedParentMessage.Msg = ""
		msg.QuotedParentMessage.Attachments = nil
	}
}

// ApplyDecryptedFields copies fields from enc back onto msg. Used by
// history-service after Cipher.Decrypt. Metadata fields on
// quoted_parent_message are preserved; only body fields are filled.
func ApplyDecryptedFields(msg *cassandra.Message, enc *EncryptedFields) {
	msg.Msg = enc.Msg
	msg.Attachments = enc.Attachments
	msg.Card = enc.Card
	msg.CardAction = enc.CardAction
	if enc.QuotedParentContent != nil {
		if msg.QuotedParentMessage == nil {
			msg.QuotedParentMessage = &cassandra.QuotedParentMessage{}
		}
		msg.QuotedParentMessage.Msg = enc.QuotedParentContent.Msg
		msg.QuotedParentMessage.Attachments = enc.QuotedParentContent.Attachments
	}
}
