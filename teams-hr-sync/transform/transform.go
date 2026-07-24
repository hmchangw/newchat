// Package transform holds the injectable seams between the Graph walk and
// the published wire model — Mapper (Graph objects → domain) and
// EmployeeUserConverter (Employee → User) — plus their default impls and the
// change-label constants. Swapping shapes, naming conventions, or change
// semantics happens here, never in the service.
package transform

import (
	"strings"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/msgraph"
)

// Mapper maps Graph objects into the domain; owns the name-mapping, org
// placement, and the employeeId derivation. An EmployeeFromMember result with
// an empty Account is unmappable (no usable UPN or Graph id) and must be
// skipped by the caller.
type Mapper interface {
	OrgFromGroup(g msgraph.GroupProfile) model.IOrg
	EmployeeFromMember(m *msgraph.GraphUser, org *model.IOrg, siteID string) model.IEmployee
}

// EmployeeUserConverter maps Employee -> User for the users.upsert feed.
type EmployeeUserConverter interface {
	UserFromEmployee(*model.IEmployee) model.User
}

// DefaultMapper implements Mapper with the spec'd defaults: a group maps to
// the SECTION level; Account = lower(UPN local-part); EngName =
// givenName+" "+surname trimmed; ChineseName = displayName. Dept/division and
// the *TCName fields stay empty — the org-taxonomy source is still an open
// stub. ponytail: no OrgType — the new Org has no type field.
type DefaultMapper struct{}

func (DefaultMapper) OrgFromGroup(g msgraph.GroupProfile) model.IOrg {
	return model.IOrg{SectID: g.ID, SectName: g.DisplayName, SectDescription: g.Description}
}

func (DefaultMapper) EmployeeFromMember(m *msgraph.GraphUser, org *model.IOrg, siteID string) model.IEmployee {
	account, ok := splitUPN(m.UserPrincipalName)
	// No stable Graph id → no deterministic employeeId key → unmappable.
	if !ok || m.ID == "" {
		return model.IEmployee{}
	}
	empID := EmployeeIDFromGraphID(m.ID)
	return model.IEmployee{
		ID:             empID, // hr_employee _id = the stable derived id
		EmployeeID:     empID,
		Account:        account,
		EngName:        strings.TrimSpace(m.GivenName + " " + m.Surname),
		ChineseName:    m.DisplayName,
		Mail:           m.Mail,
		MailNickname:   m.MailNickname,
		UserType:       m.UserType,
		AccountEnabled: m.AccountEnabled,
		SiteID:         siteID,
		IOrg:           *org,
	}
}

// EmployeeIDFromGraphID derives a deterministic 17-char base62 id (native-user shape)
// from the immutable Graph object id, so the downstream employeeId-keyed upsert resolves
// the same user on every sync instead of colliding on a blank AAD attribute. Must match
// teamsmigrate.EmployeeIDFromGraphID so message-migration and HR-sync agree on one id.
func EmployeeIDFromGraphID(graphID string) string {
	return idgen.DeterministicID([]byte(graphID))
}

// DefaultConverter copies the identity fields only; every other User field
// stays zero — the downstream persister owns defaults/merging.
type DefaultConverter struct{}

func (DefaultConverter) UserFromEmployee(e *model.IEmployee) model.User {
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
