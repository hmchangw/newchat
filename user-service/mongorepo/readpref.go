package mongorepo

import "go.mongodb.org/mongo-driver/v2/mongo/readpref"

// Option customizes repo construction.
type Option func(*settings)

type settings struct {
	readPref          *readpref.ReadPref
	showTeamsRoom     bool
	showTeamsAccounts map[string]bool
}

// WithReadPreference routes each repo's staleness-tolerant reads to rp; authz,
// dedup, read-after-write reads, and writes keep the default. Nil is a no-op.
func WithReadPreference(rp *readpref.ReadPref) Option {
	return func(s *settings) { s.readPref = rp }
}

// WithShowTeamsRoom controls whether SubscriptionRepo's list/count queries
// include Teams-migrated rooms (origin "teams"); false (the default) excludes
// them. Only consumed by SubscriptionRepo.
func WithShowTeamsRoom(show bool) Option {
	return func(s *settings) { s.showTeamsRoom = show }
}

// WithShowTeamsAccounts allowlists accounts that see Teams-migrated rooms even when
// SHOW_TEAMS_ROOM is false — an ops-managed set (SHOW_TEAMS_ROOM_ACCOUNTS). Empty is a no-op.
func WithShowTeamsAccounts(accounts []string) Option {
	return func(s *settings) {
		if len(accounts) == 0 {
			return
		}
		s.showTeamsAccounts = make(map[string]bool, len(accounts))
		for _, a := range accounts {
			if a != "" {
				s.showTeamsAccounts[a] = true
			}
		}
	}
}

func applyOptions(opts []Option) settings {
	var s settings
	for _, o := range opts {
		o(&s)
	}
	return s
}
