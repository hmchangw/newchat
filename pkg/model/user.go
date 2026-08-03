package model

import (
	"fmt"
	"strings"
	"sync/atomic"
)

// UserRole is a platform-level role flag on the User record.
// Empty Roles reads as ["user"]; positive markers are "admin" and "bot".
type UserRole string

const (
	UserRoleAdmin UserRole = "admin"
	UserRoleUser  UserRole = "user"
	UserRoleBot   UserRole = "bot"
)

// PasswordCredentials carries the bcrypt hash material for password-based
// login (bots and admins). The hash is stored at the legacy Rocket.Chat path
// users.services.password.bcrypt so a migration can rsync passwords without
// remapping. `json:"-"` keeps the hash out of every outbound payload — it is
// only ever read by botplatform-service from Mongo.
type PasswordCredentials struct {
	Bcrypt string `json:"-" bson:"bcrypt,omitempty"`
}

// IsZero lets `bson:"...,omitempty"` drop an empty credential block: mongo-driver
// v2 only omits zero-value structs that satisfy bson.Zeroer, so without this a
// passwordless user would still marshal an empty `password` sub-document.
func (p PasswordCredentials) IsZero() bool {
	return p.Bcrypt == ""
}

// Services groups nested credential blocks under users.services. New auth
// methods (e.g. resume tokens, future SSO bindings) extend this struct.
type Services struct {
	Password PasswordCredentials `json:"-" bson:"password,omitempty"`
}

// IsZero lets `bson:"...,omitempty"` drop an empty `services` block (see
// PasswordCredentials.IsZero).
func (s Services) IsZero() bool {
	return s.Password.IsZero()
}

type User struct {
	ID                    string     `json:"id"                              bson:"_id"`
	Account               string     `json:"account"                         bson:"account"`
	SiteID                string     `json:"siteId"                          bson:"siteId"`
	SectID                string     `json:"sectId"                          bson:"sectId"`
	SectName              string     `json:"sectName"                        bson:"sectName"`
	SectTCName            string     `json:"sectTCName"                      bson:"sectTCName"`
	SectDescription       string     `json:"sectDescription"                 bson:"sectDescription"`
	DeptID                string     `json:"deptId"                          bson:"deptId"`
	DeptName              string     `json:"deptName"                        bson:"deptName"`
	DeptTCName            string     `json:"deptTCName"                      bson:"deptTCName"`
	DeptDescription       string     `json:"deptDescription"                 bson:"deptDescription"`
	EngName               string     `json:"engName"                         bson:"engName"`
	ChineseName           string     `json:"chineseName"                     bson:"chineseName"`
	EmployeeID            string     `json:"employeeId"                      bson:"employeeId"`
	StatusIsShow          bool       `json:"statusIsShow"                    bson:"statusIsShow"`
	StatusText            string     `json:"statusText"                      bson:"statusText"`
	Roles                 []UserRole `json:"roles,omitempty"                 bson:"roles,omitempty"`
	RequirePasswordChange bool       `json:"requirePasswordChange,omitempty" bson:"requirePasswordChange,omitempty"`
	// Active is the account-enabled flag: nil (field absent) or true means the
	// account is active; only a stored active:false deactivates. `json:"-"` keeps
	// it out of every outbound payload — read it via IsActive.
	Active   *bool    `json:"-" bson:"active,omitempty"`
	Services Services `json:"-" bson:"services,omitempty"`
	// Settings is the per-user client-preferences sub-document; nil = never set.
	Settings *UserSettings `json:"settings,omitempty" bson:"settings,omitempty"`
	// Chatlist is the per-user section-definition overlay; nil = never customized.
	Chatlist *ChatlistState `json:"chatlist,omitempty" bson:"chatlist,omitempty"`
}

// String formats a User for log lines, deliberately omitting the bcrypt hash
// so a stray %v / %+v / structured log call never carries credential material
// to disk.
func (u User) String() string {
	return fmt.Sprintf("User{ID:%q Account:%q SiteID:%q Roles:%v}",
		u.ID, u.Account, u.SiteID, u.Roles)
}

// IsActive reports whether the user's account is active. A nil user is not
// active; on an existing user a nil (missing) Active field or an explicit true
// counts as active — only a stored active:false deactivates. Single source of
// truth for the missing-means-active rule.
func (u *User) IsActive() bool {
	return u != nil && (u.Active == nil || *u.Active)
}

// IsPlatformAdmin reports whether u holds the platform admin role. Nil-safe.
func IsPlatformAdmin(u *User) bool {
	if u == nil {
		return false
	}
	for _, r := range u.Roles {
		if r == UserRoleAdmin {
			return true
		}
	}
	return false
}

// platformAdminAccountPrefixDefault is the built-in default, overridden at
// startup via SetPlatformAdminAccountPrefix (from ADMIN_ACCT_PREFIX).
const platformAdminAccountPrefixDefault = "p_admin"

// platformAdminAccountPrefix holds the active prefix — atomic so concurrent
// reads don't race the single startup write.
var platformAdminAccountPrefix = func() *atomic.Pointer[string] {
	p := &atomic.Pointer[string]{}
	def := platformAdminAccountPrefixDefault
	p.Store(&def)
	return p
}()

// SetPlatformAdminAccountPrefix overrides the platform-admin prefix at startup.
// An empty prefix is rejected: it would match every account.
func SetPlatformAdminAccountPrefix(prefix string) error {
	if prefix == "" {
		return fmt.Errorf("platform-admin account prefix must not be empty")
	}
	platformAdminAccountPrefix.Store(&prefix)
	return nil
}

// PlatformAdminAccountPrefix returns the active platform-admin pseudo-account
// prefix (the default unless overridden via SetPlatformAdminAccountPrefix).
func PlatformAdminAccountPrefix() string {
	return *platformAdminAccountPrefix.Load()
}

// IsPlatformAdminAccount reports whether account is the platform-admin
// pseudo-account (the PlatformAdminAccountPrefix() prefix, default
// "p_admin"). That pseudo-account has a user record but no app: it counts
// into a room's appCount, is excluded from read-receipt floors, cannot
// own a room, and a DM with it is a botDM. Other "p_" names are ordinary QA/test
// users — this returns false for them.
func IsPlatformAdminAccount(account string) bool {
	return strings.HasPrefix(account, PlatformAdminAccountPrefix())
}

// HasLoginRole reports whether the role slice contains a role that may
// password-login through /v1/login (bot or admin). Single source of truth so
// the rule does not drift between portal-service's fail-fast gate and
// botplatform-service's authoritative gate.
func HasLoginRole(roles []UserRole) bool {
	for _, r := range roles {
		if r == UserRoleAdmin || r == UserRoleBot {
			return true
		}
	}
	return false
}

// ContainsBotRole reports whether the role slice contains the bot role.
// Used by portal-service's bot-login feature-flag gate.
func ContainsBotRole(roles []UserRole) bool {
	for _, r := range roles {
		if r == UserRoleBot {
			return true
		}
	}
	return false
}

// IsBot reports whether account is a bot account (a ".bot" suffix).
func IsBot(account string) bool {
	return strings.HasSuffix(account, ".bot")
}

// DisplayName renders the user's display label for Drive ownership metadata:
// the account when either name is missing, the English name when both names are
// identical, otherwise "<engName> <chineseName>".
func (u *User) DisplayName() string {
	switch {
	case u == nil:
		return ""
	case u.EngName == "" || u.ChineseName == "":
		return u.Account
	case u.EngName == u.ChineseName:
		return u.EngName
	default:
		return u.EngName + " " + u.ChineseName
	}
}
