// Package transform holds the injectable seams between the Graph walk and
// the published wire model — Mapper (Graph objects → domain) and
// EmployeeUserConverter (Employee → User) — plus their default impls and the
// change-label constants. Swapping shapes, naming conventions, or change
// semantics happens here, never in the service.
package transform

import (
	"crypto/sha256"
	"strings"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/msgraph"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// SourceTeams tags rows this producer owns; other sources' rows are never
// quit by this sync.
const SourceTeams = "teams"

// Mapper maps Graph objects into the domain; owns the name-mapping, org
// placement, and the employeeId derivation. An EmployeeFromMember result with
// an empty Account is unmappable (no usable UPN or Graph id) and must be
// skipped by the caller.
type Mapper interface {
	OrgFromGroup(g msgraph.GroupProfile) model.Org
	EmployeeFromMember(m *msgraph.GraphUser, org *model.Org, siteID string) model.Employee
}

// EmployeeUserConverter maps Employee -> User for the users.upsert feed.
type EmployeeUserConverter interface {
	UserFromEmployee(*model.Employee) model.User
}

// DefaultMapper implements Mapper with the spec'd defaults: a group maps to
// the SECTION level; Account = lower(UPN local-part); EngName =
// givenName+" "+surname trimmed; ChineseName = displayName. Dept/division and
// the *TCName fields stay empty — the org-taxonomy source is still an open
// stub. ponytail: no OrgType — the new Org has no type field.
type DefaultMapper struct{}

func (DefaultMapper) OrgFromGroup(g msgraph.GroupProfile) model.Org {
	return model.Org{SectID: g.ID, SectName: g.DisplayName, SectDescription: g.Description}
}

func (DefaultMapper) EmployeeFromMember(m *msgraph.GraphUser, org *model.Org, siteID string) model.Employee {
	account, ok := splitUPN(m.UserPrincipalName)
	// No stable Graph id → no deterministic employeeId key → unmappable.
	if !ok || m.ID == "" {
		return model.Employee{}
	}
	return model.Employee{
		EmployeeID:  EmployeeIDFromGraphID(m.ID),
		Account:     account,
		EngName:     strings.TrimSpace(m.GivenName + " " + m.Surname),
		ChineseName: m.DisplayName,
		SiteID:      siteID,
		Source:      SourceTeams,
		Org:         *org,
	}
}

// EmployeeIDFromGraphID derives a deterministic bson.ObjectID (24-hex) from the
// immutable Graph object id, so the downstream employeeId-keyed upsert resolves
// the same user on every sync instead of colliding on a blank AAD attribute.
func EmployeeIDFromGraphID(graphID string) string {
	sum := sha256.Sum256([]byte(graphID))
	var oid bson.ObjectID
	copy(oid[:], sum[:12])
	return oid.Hex()
}

// DefaultConverter copies the identity fields only; every other User field
// stays zero — the downstream persister owns defaults/merging.
type DefaultConverter struct{}

func (DefaultConverter) UserFromEmployee(e *model.Employee) model.User {
	return model.User{
		Account:     e.Account,
		SiteID:      e.SiteID,
		EngName:     e.EngName,
		ChineseName: e.ChineseName,
		EmployeeID:  e.EmployeeID,
	}
}

// splitUPN extracts a userPrincipalName's lowercased local part (the account).
// ok is false when there is no non-empty local part (no '@', or '@' first).
func splitUPN(upn string) (account string, ok bool) {
	at := strings.Index(upn, "@")
	if at <= 0 {
		return "", false
	}
	return strings.ToLower(upn[:at]), true
}
