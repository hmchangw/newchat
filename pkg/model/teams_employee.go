package model

// IOrg mirrors Org's fields so IEmployee can embed the org hierarchy without
// depending on Org (a downstream repurposes the generic name). Tags are
// identical to Org — same JSON + bson shape on the wire.
type IOrg struct {
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

// IChangeType marks how a row moved since the last sync.
type IChangeType string

const (
	IChangeTypeNewHire IChangeType = "new_hire"
	IChangeTypeUpdate  IChangeType = "update"
)

// IEmployee is the shared HR-feed row a downstream service maps into User.
// IOrg embeds inline so the nine org fields serialize flat alongside the
// identity fields — one row feeds both the ES org index and the hr_employee store.
type IEmployee struct {
	IOrg `bson:",inline"`
	// ID is the hr_employee document _id; json:"-" keeps it off the upsert wire.
	ID             string `json:"-"              bson:"_id,omitempty"`
	EmployeeID     string `json:"employeeId"     bson:"employeeId"`
	Account        string `json:"account"        bson:"account"`
	EngName        string `json:"engName"        bson:"engName"`
	ChineseName    string `json:"chineseName"    bson:"chineseName"`
	Mail           string `json:"mail"           bson:"mail"`
	MailNickname   string `json:"mailNickname"   bson:"mailNickname"`
	UserType       string `json:"userType"       bson:"userType"`
	AccountEnabled bool   `json:"accountEnabled" bson:"accountEnabled"`
	SiteID         string `json:"siteId"         bson:"siteId"`
}

// IEmployeeWithChange is one employees.upsert element (bare array, no wrapper).
type IEmployeeWithChange struct {
	IEmployee
	ChangeType IChangeType `json:"changeType,omitempty"`
}

// IUserWithChange is one users.upsert element (bare array); embeds the real User.
type IUserWithChange struct {
	User
	ChangeType IChangeType `json:"changeType,omitempty"`
}

// IHRSyncEmployeeQuitBatch lists departed accounts for one site
// (subject chat.hr.{siteID}.employees.quit). Quit stays a wrapper; the two
// upserts go bare.
type IHRSyncEmployeeQuitBatch struct {
	Timestamp int64    `json:"timestamp"`
	SiteID    string   `json:"siteId"`
	Accounts  []string `json:"accounts"`
}
