package models

// StatusGetByNameRequest is the body of status.getByName.
type StatusGetByNameRequest struct {
	Name string `json:"name"`
}

// StatusSetRequest is the body of status.set (Text ≤ 512 bytes).
type StatusSetRequest struct {
	Text   string `json:"text"`
	IsShow *bool  `json:"isShow,omitempty"`
}

// UserStatusView is the response of status.getByName / status.set.
// StatusIsShow is always present (no omitempty) — never-set users decode to false.
type UserStatusView struct {
	Account      string `json:"account"`
	StatusText   string `json:"statusText"`
	StatusIsShow bool   `json:"statusIsShow"`
	ChineseName  string `json:"chineseName,omitempty"`
	EngName      string `json:"engName,omitempty"`
}
