package model

import (
	"fmt"
	"time"
)

// SSOToken is one user's stored token pair in the sso_tokens collection (legacy field
// names kept; IDToken is the Keycloak access token). NOTE: do NOT bson-decode the
// collection directly into this type — `idTokenExp` is a decimal STRING column, so
// unmarshalling it into IDTokenExp (int64) fails. The mongorepo uses an internal
// string-typed doc and converts at the boundary; the bson tags here are for reference only.
type SSOToken struct {
	ID           string    `json:"id"          bson:"_id"`
	Username     string    `json:"username"    bson:"username"`
	IDToken      string    `json:"-"           bson:"idToken"`
	IDTokenExp   int64     `json:"idTokenExp"  bson:"idTokenExp"` // in-memory millis; persisted as a decimal string (the repo converts)
	RefreshToken string    `json:"-"           bson:"refreshToken"`
	UpdatedAt    time.Time `json:"updatedAt"   bson:"_updatedAt"`
}

// String redacts both token values so a stray %v never logs credentials.
func (s SSOToken) String() string {
	return fmt.Sprintf("SSOToken{ID:%q Username:%q IDTokenExp:%d}", s.ID, s.Username, s.IDTokenExp)
}
