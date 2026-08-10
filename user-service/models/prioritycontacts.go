package models

// PriorityContactItem.Type values — the explicit discriminator, so the client never
// has to infer a row's kind from the ".bot" suffix or from which fields came back set.
const (
	PriorityContactTypeUser = "user"
	PriorityContactTypeBot  = "bot"
)

// PriorityContactMutateRequest is the body of settings.priorityContacts.add and
// settings.priorityContacts.remove.
type PriorityContactMutateRequest struct {
	ContactAccount string `json:"contactAccount"`
}

// PriorityContactUser carries the HR-directory fields rendered for a regular-user
// priority contact.
type PriorityContactUser struct {
	EngName     string `json:"engName"`
	ChineseName string `json:"chineseName"`
	EmployeeID  string `json:"employeeId"`
	SectName    string `json:"sectName"`
}

// PriorityContactApp carries the app name rendered for a bot priority contact.
type PriorityContactApp struct {
	Name string `json:"name"`
}

// PriorityContactItem is one row: the account, an explicit kind, and at most one
// populated detail object. Both detail pointers are nil when the account no longer
// resolves (deactivated user, deleted app) — the row still carries account+type.
type PriorityContactItem struct {
	Account string               `json:"account"`
	Type    string               `json:"type"`
	User    *PriorityContactUser `json:"user,omitempty"`
	App     *PriorityContactApp  `json:"app,omitempty"`
}

// PriorityContactsResponse is the reply for all three priority-contact RPCs: the
// full enriched list in stored order.
type PriorityContactsResponse struct {
	Contacts []PriorityContactItem `json:"contacts"`
}
