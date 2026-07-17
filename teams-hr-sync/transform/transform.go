// Package transform holds the injectable seams between the Graph walk and
// the published wire model — Mapper (Graph objects → domain) and
// EmployeeUserConverter (Employee → User) — plus their default impls and the
// change-label constants. Swapping shapes, naming conventions, or change
// semantics happens here, never in the service.
package transform

import (
	"strings"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/msgraph"
)

// Change labels — the downstream contract's values live here so swapping
// semantics is a transform-layer change.
const (
	ChangeCreated = "created"
	ChangeUpdated = "updated"
)

// SourceTeams tags rows this producer owns; other sources' rows are never
// quit by this sync.
const SourceTeams = "teams"

// Mapper maps Graph objects into the domain; owns the name-mapping and
// Org.Type decisions. An EmployeeFromMember result with an empty Account is
// unmappable (no usable UPN) and must be skipped by the caller.
type Mapper interface {
	OrgFromGroup(g msgraph.GroupProfile) model.Org
	EmployeeFromMember(m *msgraph.GraphUser, org model.Org, siteID string) model.Employee
}

// EmployeeUserConverter maps Employee -> User for the users.upsert feed.
type EmployeeUserConverter interface {
	UserFromEmployee(*model.Employee) model.User
}

// DefaultMapper implements Mapper with the spec'd defaults: Org from the
// group profile + its OrgType; Account = lower(UPN local-part); EngName =
// givenName+" "+surname trimmed; ChineseName = displayName.
type DefaultMapper struct {
	OrgType string
}

func (d DefaultMapper) OrgFromGroup(g msgraph.GroupProfile) model.Org {
	return model.Org{ID: g.ID, Name: g.DisplayName, Description: g.Description, Type: d.OrgType}
}

func (DefaultMapper) EmployeeFromMember(m *msgraph.GraphUser, org model.Org, siteID string) model.Employee {
	account, ok := splitUPN(m.UserPrincipalName)
	if !ok {
		return model.Employee{}
	}
	return model.Employee{
		EmployeeID:  m.EmployeeID,
		Account:     account,
		EngName:     strings.TrimSpace(m.GivenName + " " + m.Surname),
		ChineseName: m.DisplayName,
		SiteID:      siteID,
		Source:      SourceTeams,
		Org:         org,
	}
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
