package main

import (
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/model"
)

// rocketchatMessage is the subset of a RocketChat message doc we consume. Decoded from the
// connector's relaxed extended JSON via bson.UnmarshalExtJSON (handles $date/$oid).
type rocketchatMessage struct {
	ID       string     `bson:"_id"`
	RID      string     `bson:"rid"`
	Msg      string     `bson:"msg"`
	TS       time.Time  `bson:"ts"`
	EditedAt *time.Time `bson:"editedAt"`
	T        string     `bson:"t"`
	TMID     string     `bson:"tmid"`
	U        struct {
		ID       string `bson:"_id"`
		Username string `bson:"username"`
		Name     string `bson:"name"`
	} `bson:"u"`
	// Federation.Origin is the remote server a message was authored on; absent for local messages.
	// Only local messages are migrated — foreign copies arrive via the new app's own federation (would double-deliver).
	Federation struct {
		Origin string `bson:"origin"`
	} `bson:"federation"`
}

// decodeRocketchatMessage parses the connector's opaque relaxed-extJSON document.
func decodeRocketchatMessage(raw []byte) (*rocketchatMessage, error) {
	var doc rocketchatMessage
	if err := bson.UnmarshalExtJSON(raw, false, &doc); err != nil {
		return nil, fmt.Errorf("decode rocketchat message: %w", err)
	}
	return &doc, nil
}

// isSoftDeleted reports whether the source doc represents a soft-deleted message (t == softDeleteType).
func isSoftDeleted(rc *rocketchatMessage, softDeleteType string) bool {
	return rc.T == softDeleteType
}

// isSystemMessage reports whether the doc is a system/event message — any non-empty t other than
// the soft-delete marker. Their msg holds system text/ciphertext, not user content, so they're skipped.
func isSystemMessage(rc *rocketchatMessage, softDeleteType string) bool {
	return rc.T != "" && rc.T != softDeleteType
}

// isForeignOrigin reports whether the message was authored at a remote site. The connector's $match
// drops foreign insert/replace at the source; this guards the update path (no fullDocument to match on there).
func isForeignOrigin(rc *rocketchatMessage) bool {
	return rc.Federation.Origin != ""
}

// mapToMessage translates a RocketChat doc into the new-stack model.Message. The _id is kept
// verbatim (17-char RocketChat id, accepted by idgen.IsValidMessageID).
func mapToMessage(rc *rocketchatMessage) model.Message {
	return model.Message{
		ID:                    rc.ID,
		RoomID:                rc.RID,
		UserID:                rc.U.ID,
		UserAccount:           rc.U.Username,
		UserDisplayName:       rc.U.Name,
		Content:               rc.Msg,
		CreatedAt:             rc.TS,
		EditedAt:              rc.EditedAt,
		ThreadParentMessageID: rc.TMID,
	}
}
