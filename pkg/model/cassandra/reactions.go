package cassandra

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/hmchangw/chat/pkg/displayfmt"
)

// ReactionKey is the map-key UDT for Message.Reactions.
type ReactionKey struct {
	// Emoji must be NFC-normalised by writers; enforcement is in
	// pkg/emoji.Canonicalize (the single chokepoint history-service uses
	// before binding a value into this field; format-only, not this type).
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
	// DisplayName is a transient carrier populated only when a Reactions value was
	// decoded from the client wire form (see UnmarshalJSON). MarshalJSON prefers it
	// over composing from EngName/ChnName. json:"-" keeps it off the wire directly;
	// cql:"-" keeps gocql from binding/scanning it into the Cassandra UDT.
	DisplayName string `json:"-" cql:"-"`
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
		dn := v.DisplayName
		if dn == "" {
			dn = displayfmt.CombineWithFallback(v.EngName, v.ChnName, k.UserAccount)
		}
		staged[k.Emoji] = append(staged[k.Emoji], reactorWithTime{
			user: reactionUser{
				Account:     k.UserAccount,
				DisplayName: dn,
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

// UnmarshalJSON is the inverse of MarshalJSON: it reads the wire shape
// map<emoji, [{account, displayName}]> back into the struct-keyed map. Without it
// the default decoder rejects the JSON object because the map key is a struct
// (ReactionKey), not a string/int/TextUnmarshaler — which is what broke decoding
// any cassandra.Message that carries reactions.
//
// The wire form is lossy — userId, chnName and reactedAt are not transmitted — so
// a decoded ReactorInfo carries only Account plus the already-composed display
// name (in the transient DisplayName field, which MarshalJSON re-emits verbatim).
// A synthetic per-emoji ReactedAt preserves each emoji's on-wire FIFO reactor order
// across marshal→unmarshal→marshal. "null" → nil map; "{}" → non-nil empty map.
func (r *Reactions) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		*r = nil
		return nil
	}
	var grouped map[string][]reactionUser
	if err := json.Unmarshal(data, &grouped); err != nil {
		return fmt.Errorf("decode reactions: %w", err)
	}
	out := make(Reactions, len(grouped))
	for emoji, users := range grouped {
		for i, u := range users {
			out[ReactionKey{Emoji: emoji, UserAccount: u.Account}] = ReactorInfo{
				Account:     u.Account,
				DisplayName: u.DisplayName,
				// Index-based so MarshalJSON's (reactedAt, account) sort replays the
				// original array order; distinct per position within each emoji bucket.
				ReactedAt: time.UnixMilli(int64(i)),
			}
		}
	}
	*r = out
	return nil
}
