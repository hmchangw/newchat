package main

import "github.com/hmchangw/chat/pkg/displayfmt"

// orgDisplayUser is the projected user row used to resolve org-member display
// names. Only the dept/sect identity and name fields are read.
type orgDisplayUser struct {
	DeptID          string `bson:"deptId"`
	SectID          string `bson:"sectId"`
	DeptName        string `bson:"deptName"`
	DeptTCName      string `bson:"deptTCName"`
	SectName        string `bson:"sectName"`
	SectTCName      string `bson:"sectTCName"`
	DeptDescription string `bson:"deptDescription"`
	SectDescription string `bson:"sectDescription"`
}

// orgDisplayAgg is the per-org rollup that drives the member.sectName display
// string and member.memberCount. It mirrors the fields the prior in-pipeline
// $group produced.
type orgDisplayAgg struct {
	isDept          bool
	deptName        string
	deptTCName      string
	sectName        string
	sectTCName      string
	deptDescription string
	sectDescription string
	memberCount     int
}

// buildOrgDisplay rolls up the candidate users (those whose deptId or sectId
// matches one of orgIDs) into a per-org aggregate. It reproduces the semantics
// of the previous correlated $lookup + $group exactly:
//
//   - a user matches an org via deptId (a "dept match", which sets isDept) or
//     via sectId when its deptId does not also match (a "sect match");
//   - deptId == sectId is counted once, as a dept match (the old $or matched
//     the document once and classified it by deptId == orgId);
//   - branch names take the lexicographic max within their branch, matching the
//     old $max over the gated name expressions;
//   - memberCount counts each matching user once per org.
//
// Doing this Go-side lets the database query be a single index-backed batch
// (deptId/sectId $in orgIDs) instead of a per-row collection scan.
func buildOrgDisplay(orgIDs []string, users []orgDisplayUser) map[string]*orgDisplayAgg {
	want := make(map[string]struct{}, len(orgIDs))
	for _, id := range orgIDs {
		if id != "" {
			want[id] = struct{}{}
		}
	}
	out := make(map[string]*orgDisplayAgg, len(orgIDs))
	get := func(id string) *orgDisplayAgg {
		a := out[id]
		if a == nil {
			a = &orgDisplayAgg{}
			out[id] = a
		}
		return a
	}
	for i := range users {
		u := &users[i]
		if _, ok := want[u.DeptID]; ok {
			a := get(u.DeptID)
			a.isDept = true
			a.memberCount++
			if u.DeptName > a.deptName {
				a.deptName = u.DeptName
			}
			if u.DeptTCName > a.deptTCName {
				a.deptTCName = u.DeptTCName
			}
			if u.DeptDescription > a.deptDescription {
				a.deptDescription = u.DeptDescription
			}
		}
		// Sect match only when sectId differs from a dept match on the same user,
		// so a deptId == sectId user is not double-counted (dept takes precedence).
		if _, ok := want[u.SectID]; ok && u.SectID != u.DeptID {
			a := get(u.SectID)
			a.memberCount++
			if u.SectName > a.sectName {
				a.sectName = u.SectName
			}
			if u.SectTCName > a.sectTCName {
				a.sectTCName = u.SectTCName
			}
			if u.SectDescription > a.sectDescription {
				a.sectDescription = u.SectDescription
			}
		}
	}
	return out
}

// orgDisplaySectName renders an org member's sectName display string from its
// rollup, applying the dept-first-with-fallback tiebreak so the output matches
// room-worker's sys-message formatter byte-for-byte: prefer dept names when a
// dept match exists and carries a non-empty name, otherwise fall back to sect
// names, and finally to the orgID when both are empty. A nil aggregate (no
// users matched the org) falls back to the orgID.
func orgDisplaySectName(a *orgDisplayAgg, orgID string) string {
	var name, tcName string
	if a != nil {
		if a.isDept && (a.deptName != "" || a.deptTCName != "") {
			name, tcName = a.deptName, a.deptTCName
		}
		if name == "" && tcName == "" {
			name, tcName = a.sectName, a.sectTCName
		}
	}
	return displayfmt.CombineWithFallback(name, tcName, orgID)
}

// orgDisplayDescription returns the description from the same branch that supplies
// the name (dept-first, like orgDisplaySectName); "" if that branch has none, no orgID fallback.
func orgDisplayDescription(a *orgDisplayAgg) string {
	if a == nil {
		return ""
	}
	if a.isDept && (a.deptName != "" || a.deptTCName != "") {
		return a.deptDescription
	}
	if a.sectName != "" || a.sectTCName != "" {
		return a.sectDescription
	}
	return ""
}
