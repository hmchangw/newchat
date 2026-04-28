package service

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/history-service/internal/models"
)

func TestCanModify(t *testing.T) {
	tests := []struct {
		name    string
		msg     *models.Message
		account string
		want    bool
	}{
		{
			name:    "sender matches — allowed",
			msg:     &models.Message{Sender: models.Participant{Account: "alice"}},
			account: "alice",
			want:    true,
		},
		{
			name:    "different account — not allowed",
			msg:     &models.Message{Sender: models.Participant{Account: "alice"}},
			account: "bob",
			want:    false,
		},
		{
			name:    "empty sender account — not allowed",
			msg:     &models.Message{Sender: models.Participant{Account: ""}},
			account: "",
			want:    false,
		},
		{
			name:    "empty caller account — not allowed even if sender is set",
			msg:     &models.Message{Sender: models.Participant{Account: "alice"}},
			account: "",
			want:    false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := canModify(tt.msg, tt.account)
			assert.Equal(t, tt.want, got)
		})
	}
}
