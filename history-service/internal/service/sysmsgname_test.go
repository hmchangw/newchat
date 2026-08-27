package service

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/history-service/internal/models"
)

func TestExtractRemovedAccount(t *testing.T) {
	tests := []struct {
		name        string
		msgType     string
		msg         string
		wantAccount string
		wantOK      bool
	}{
		{
			name:        "legacy row yields the account prefix",
			msgType:     "members_removed",
			msg:         "bob has been removed from the channel.",
			wantAccount: "bob",
			wantOK:      true,
		},
		{
			name:        "account containing spaces is kept whole",
			msgType:     "members_removed",
			msg:         "bob smith has been removed from the channel.",
			wantAccount: "bob smith",
			wantOK:      true,
		},
		{
			name:    "modern member_removed type is not rewritten",
			msgType: "member_removed",
			msg:     "bob has been removed from the channel.",
			wantOK:  false,
		},
		{
			name:    "ordinary user message with no type is never touched",
			msgType: "",
			msg:     "bob has been removed from the channel.",
			wantOK:  false,
		},
		{
			name:    "missing trailing period does not match",
			msgType: "members_removed",
			msg:     "bob has been removed from the channel",
			wantOK:  false,
		},
		{
			name:    "suffix with no account prefix is not a rewrite candidate",
			msgType: "members_removed",
			msg:     " has been removed from the channel.",
			wantOK:  false,
		},
		{
			name:    "empty msg",
			msgType: "members_removed",
			msg:     "",
			wantOK:  false,
		},
		{
			name:    "unrelated text on the legacy type is left alone",
			msgType: "members_removed",
			msg:     "something else entirely",
			wantOK:  false,
		},
		{
			name:    "suffix in the middle rather than at the end",
			msgType: "members_removed",
			msg:     "bob has been removed from the channel. and then rejoined",
			wantOK:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := models.Message{Type: tc.msgType, Msg: tc.msg}
			account, ok := extractRemovedAccount(&m)
			assert.Equal(t, tc.wantOK, ok)
			assert.Equal(t, tc.wantAccount, account)
		})
	}
}
