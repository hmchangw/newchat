package cassandra

import (
	"encoding/json"
	"sort"
	"time"

	"github.com/hmchangw/chat/pkg/displayfmt"
)

// ReactionKey is the map-key UDT for Message.Reactions.
type ReactionKey struct {
	// Emoji must be NFC-normalised by writers; enforcement is in
	// pkg/emoji.Validator (the single chokepoint history-service uses
	// before binding a value into this field), not this type.
	Emoji       string `json:"emoji"       cql:"emoji"`
	UserAccount string `json:"userAccount" cql:"user_account"`
}

// ReactorInfo is the map-value UDT for Message.Reactions.
type ReactorInfo struct {
	UserID    string    `json:"userId"    cql:"user_id"`
	EngName   string    `json:"engName"   cql:"eng_name"`
	ChnName   string    `json:"chnName"   cql:"chn_name"`
	Account   string    `json:"account"   cql:"account"`
	ReactedAt time.Time `json:"reactedAt" cql:"reacted_at"`
}

// Reactions is the in-row reaction map. Stored as map[(emoji,userAccount)]reactor;
// emitted on the wire grouped by emoji with composed display names — see MarshalJSON.
type Reactions map[ReactionKey]ReactorInfo

// reactionUser is the per-reactor record emitted on the wire.
type reactionUser struct {
	Account     string `json:"account"`
	DisplayName string `json:"displayName"`
}

// reactorWithTime carries ReactedAt only for the sort step; it never reaches the wire.
type reactorWithTime struct {
	user      reactionUser
	reactedAt time.Time
}

// MarshalJSON groups reactors by emoji and emits map<emoji, [{account, displayName}]>.
// Per-emoji arrays are sorted by reactedAt ascending (FIFO — oldest reaction first),
// matching the old MongoDB insertion-order behaviour; account ASC breaks same-millisecond ties.
// Outer JSON object key order is unspecified (the FE applies its own emoji ordering).
// nil → "null"; empty → "{}".
func (r Reactions) MarshalJSON() ([]byte, error) {
	if r == nil {
		return []byte("null"), nil
	}
	staged := make(map[string][]reactorWithTime, len(r))
	for k, v := range r {
		staged[k.Emoji] = append(staged[k.Emoji], reactorWithTime{
			user: reactionUser{
				Account:     k.UserAccount,
				DisplayName: displayfmt.CombineWithFallback(v.EngName, v.ChnName, k.UserAccount),
			},
			reactedAt: v.ReactedAt,
		})
	}
	grouped := make(map[string][]reactionUser, len(staged))
	for emoji, entries := range staged {
		sort.Slice(entries, func(i, j int) bool {
			if !entries[i].reactedAt.Equal(entries[j].reactedAt) {
				return entries[i].reactedAt.Before(entries[j].reactedAt)
			}
			return entries[i].user.Account < entries[j].user.Account
		})
		users := make([]reactionUser, len(entries))
		for i, e := range entries {
			users[i] = e.user
		}
		grouped[emoji] = users
	}
	return json.Marshal(grouped)
}
