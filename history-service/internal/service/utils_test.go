package service

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/history-service/internal/models"
)

// TestQuoteInaccessible_TShowAccessWindow locks the TShow access-window check
// that consumes the server-resolved ThreadParentCreatedAt (#322). The value is
// now resolved server-side at the gatekeeper; this test guards that the
// downstream access-window decision stays correct across the parent-window
// boundary so the resolve-location change cannot silently regress visibility.
func TestQuoteInaccessible_TShowAccessWindow(t *testing.T) {
	accessSince := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	beforeWindow := accessSince.Add(-time.Hour) // parent created before access
	afterWindow := accessSince.Add(time.Hour)   // parent created after access

	tests := []struct {
		name string
		msg  *models.Message
		q    *models.QuotedParentMessage
		want bool
	}{
		{
			// criterion (b): TShow reply whose thread parent is OUTSIDE the
			// access window → inaccessible (redact), even though the quoted
			// message itself was created after access.
			name: "tshow parent before access window — inaccessible",
			msg:  &models.Message{TShow: true},
			q: &models.QuotedParentMessage{
				CreatedAt:             afterWindow,
				ThreadParentID:        "thread-parent-1",
				ThreadParentCreatedAt: &beforeWindow,
			},
			want: true,
		},
		{
			name: "tshow parent inside access window — accessible",
			msg:  &models.Message{TShow: true},
			q: &models.QuotedParentMessage{
				CreatedAt:             afterWindow,
				ThreadParentID:        "thread-parent-1",
				ThreadParentCreatedAt: &afterWindow,
			},
			want: false,
		},
		{
			// Defensive: a TShow reply that somehow reaches read-time with no
			// resolved parent timestamp is redacted conservatively to prevent a
			// pre-access leak. Post-#322 the gatekeeper always resolves this, so
			// this path is legacy-only.
			name: "tshow parent timestamp missing — redact conservatively",
			msg:  &models.Message{TShow: true},
			q: &models.QuotedParentMessage{
				CreatedAt:      afterWindow,
				ThreadParentID: "thread-parent-1",
			},
			want: true,
		},
		{
			name: "non-tshow reply, quoted message inside window — accessible",
			msg:  &models.Message{TShow: false},
			q: &models.QuotedParentMessage{
				CreatedAt:             afterWindow,
				ThreadParentID:        "thread-parent-1",
				ThreadParentCreatedAt: &beforeWindow,
			},
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, quoteInaccessible(tc.msg, tc.q, accessSince))
		})
	}
}

func TestCanModify(t *testing.T) {
	tests := []struct {
		name    string
		msg     *models.Message
		account string
		want    bool
	}{
		{
			name:    "nil message — not allowed",
			msg:     nil,
			account: "alice",
			want:    false,
		},
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
