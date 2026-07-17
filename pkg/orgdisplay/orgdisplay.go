// Package orgdisplay rolls user directory rows into the per-org display aggregates shared by room-service and room-worker.
package orgdisplay

import "github.com/hmchangw/chat/pkg/displayfmt"

// User is the projected directory row Build reads (dept/sect identity, names, descriptions).
type User struct {
	DeptID          string `bson:"deptId"`
	SectID          string `bson:"sectId"`
	DeptName        string `bson:"deptName"`
	DeptTCName      string `bson:"deptTCName"`
	SectName        string `bson:"sectName"`
	SectTCName      string `bson:"sectTCName"`
	DeptDescription string `bson:"deptDescription"`
	SectDescription string `bson:"sectDescription"`
}

// Agg is the per-org rollup, mirroring the fields the prior in-pipeline $group produced.
type Agg struct {
	IsDept          bool
	DeptName        string
	DeptTCName      string
	SectName        string
	SectTCName      string
	DeptDescription string
	SectDescription string
	MemberCount     int
}

// Build rolls users matching orgIDs into per-org aggregates, reproducing the prior
// correlated $lookup+$group exactly: dept match wins, names take the branch's lexicographic $max.
func Build(orgIDs []string, users []User) map[string]*Agg {
	want := make(map[string]struct{}, len(orgIDs))
	for _, id := range orgIDs {
		if id != "" {
			want[id] = struct{}{}
		}
	}
	out := make(map[string]*Agg, len(orgIDs))
	get := func(id string) *Agg {
		a := out[id]
		if a == nil {
			a = &Agg{}
			out[id] = a
		}
		return a
	}
	for i := range users {
		u := &users[i]
		if _, ok := want[u.DeptID]; ok {
			a := get(u.DeptID)
			a.IsDept = true
			a.MemberCount++
			if u.DeptName > a.DeptName {
				a.DeptName = u.DeptName
			}
			if u.DeptTCName > a.DeptTCName {
				a.DeptTCName = u.DeptTCName
			}
			if u.DeptDescription > a.DeptDescription {
				a.DeptDescription = u.DeptDescription
			}
		}
		// Sect branch skipped when sectId == deptId: dept takes precedence, no double count.
		if _, ok := want[u.SectID]; ok && u.SectID != u.DeptID {
			a := get(u.SectID)
			a.MemberCount++
			if u.SectName > a.SectName {
				a.SectName = u.SectName
			}
			if u.SectTCName > a.SectTCName {
				a.SectTCName = u.SectTCName
			}
			if u.SectDescription > a.SectDescription {
				a.SectDescription = u.SectDescription
			}
		}
	}
	return out
}

// Name renders the org display string (dept names first, then sect, then orgID; nil agg
// falls back to orgID), byte-for-byte matching room-worker's sys-message formatter.
func Name(a *Agg, orgID string) string {
	var name, tcName string
	if a != nil {
		if a.IsDept && (a.DeptName != "" || a.DeptTCName != "") {
			name, tcName = a.DeptName, a.DeptTCName
		}
		if name == "" && tcName == "" {
			name, tcName = a.SectName, a.SectTCName
		}
	}
	return displayfmt.CombineWithFallback(name, tcName, orgID)
}

// Code returns the org's stable code — the raw section/department name (dept-first,
// same branch Name selects) rather than the combined display string; "" if none, no orgID fallback.
func Code(a *Agg) string {
	if a == nil {
		return ""
	}
	if a.IsDept && a.DeptName != "" {
		return a.DeptName
	}
	return a.SectName
}

// Description returns the name-supplying branch's description; "" if none, no orgID fallback.
func Description(a *Agg) string {
	if a == nil {
		return ""
	}
	if a.IsDept && (a.DeptName != "" || a.DeptTCName != "") {
		return a.DeptDescription
	}
	if a.SectName != "" || a.SectTCName != "" {
		return a.SectDescription
	}
	return ""
}
