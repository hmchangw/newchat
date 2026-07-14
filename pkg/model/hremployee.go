package model

// OrgTaxonomy is the shared structured org hierarchy — the same nine fields (and
// identical json tags) as search-sync-worker's SpotlightOrgIndex, so one wire row
// feeds both the ES org index and the hr_employee store.
type OrgTaxonomy struct {
	SectID          string `json:"sectId,omitempty"          bson:"sectId,omitempty"`
	SectTCName      string `json:"sectTCName,omitempty"      bson:"sectTCName,omitempty"`
	SectName        string `json:"sectName,omitempty"        bson:"sectName,omitempty"`
	SectDescription string `json:"sectDescription,omitempty" bson:"sectDescription,omitempty"`
	DeptID          string `json:"deptId,omitempty"          bson:"deptId,omitempty"`
	DeptTCName      string `json:"deptTCName,omitempty"      bson:"deptTCName,omitempty"`
	DeptName        string `json:"deptName,omitempty"        bson:"deptName,omitempty"`
	DeptDescription string `json:"deptDescription,omitempty" bson:"deptDescription,omitempty"`
	DivisionID      string `json:"divisionId,omitempty"      bson:"divisionId,omitempty"`
}

// Employee is the shared HR row. OrgTaxonomy embeds inline so the org fields stay
// flat on the wire — the search-sync consumer decodes the flat org subset. Source
// tags the origin feed (e.g. "teams") so coexisting producers don't quit each
// other's rows.
type Employee struct {
	OrgTaxonomy `bson:",inline"`
	EmployeeID  string `json:"employeeId,omitempty" bson:"employeeId,omitempty"`
	Account     string `json:"account,omitempty"    bson:"account,omitempty"`
	Name        string `json:"name,omitempty"       bson:"name,omitempty"`
	SiteID      string `json:"siteId,omitempty"     bson:"siteId,omitempty"`
	Source      string `json:"source,omitempty"     bson:"source,omitempty"`
}

// EmployeeChange marks how a row changed since the last sync.
type EmployeeChange string

const (
	EmployeeChangeCreated EmployeeChange = "created"
	EmployeeChangeUpdated EmployeeChange = "updated"
)

// EmployeeWithChange is one employees.upsert element. Employee embeds
// anonymously so the org fields stay flat for the consumer.
type EmployeeWithChange struct {
	Employee
	// Change semantics are the downstream persister's contract (not in-repo) — confirm before wiring the diff (Phase 3).
	Change EmployeeChange `json:"change,omitempty"`
}

// EmployeesUpsertBatch matches the search-sync-worker consumer's wire shape
// (timestamp + employees); the consumer decodes each element's flat org subset.
type EmployeesUpsertBatch struct {
	Timestamp int64                `json:"timestamp"`
	Employees []EmployeeWithChange `json:"employees"`
}

// UserWithChange is one users.upsert element (no in-repo consumer yet — mirrors
// the employees pattern).
type UserWithChange struct {
	User
	Change EmployeeChange `json:"change,omitempty"`
}

// UsersUpsertBatch is the users.upsert wire shape.
type UsersUpsertBatch struct {
	Timestamp int64            `json:"timestamp"`
	Users     []UserWithChange `json:"users"`
}

// HRSyncEmployeeQuitBatch lists departed accounts for one site.
// Quit-batch shape is the downstream contract (not in-repo) — confirm; per-site (subject is chat.hr.{siteID}.employees.quit).
type HRSyncEmployeeQuitBatch struct {
	Timestamp int64    `json:"timestamp"`
	SiteID    string   `json:"siteId"`
	Accounts  []string `json:"accounts"`
}
