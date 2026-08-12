package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/session"
)

// setupPermissionsRouter wires h into a Gin engine with the same fake
// requireAdmin principal injection as setupRouter (handler_test.go), plus
// the real bodyLimit middleware, wired to only the permission routes.
func setupPermissionsRouter(h *Handler) *gin.Engine {
	r := gin.New()
	r.Use(bodyLimit(maxRequestBodyBytes))
	r.Use(func(c *gin.Context) {
		c.Set(ctxPrincipal, session.Session{
			ID:      "sess-1",
			UserID:  "admin-user-id",
			Account: "p_admin",
			SiteID:  "site-A",
			Roles:   []string{"admin"},
		})
		c.Next()
	})
	r.POST("/permissions", h.createPermissions)
	r.GET("/permissions", h.listPermissions)
	return r
}

// strPtr returns a pointer to s, for constructing permissionRequest's
// optional *string date fields in test bodies.
func strPtr(s string) *string { return &s }

// futureDate returns the civil date (fixed UTC+8 rule) days from now as
// YYYY-MM-DD, for handler test rows that must keep exercising their named
// validation branch regardless of when the suite runs — a hardcoded date
// would silently decay into the until-not-past branch once it lapses.
func futureDate(days int) string {
	return time.Now().In(tzTaipei).AddDate(0, 0, days).Format("2006-01-02")
}

// manyAccounts returns n distinct account names, "acct-0".."acct-(n-1)".
func manyAccounts(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = "acct-" + strconv.Itoa(i)
	}
	return out
}

// -------------------------------------------------------------------------
// parseWindow / displayDate / displayUntilDate — pure-function unit tests
// -------------------------------------------------------------------------

func TestParseWindow(t *testing.T) {
	fixedNow := time.Date(2026, 8, 11, 10, 0, 0, 0, time.UTC)
	todayTaipei := fixedNow.In(tzTaipei)

	tests := []struct {
		name      string
		fromStr   *string
		untilStr  *string
		wantErr   bool
		wantFrom  time.Time
		wantUntil time.Time
	}{
		{
			name:      "explicit window, from before until",
			fromStr:   strPtr("2026-09-01"),
			untilStr:  strPtr("2026-12-31"),
			wantFrom:  time.Date(2026, 9, 1, 0, 0, 0, 0, tzTaipei),
			wantUntil: time.Date(2027, 1, 1, 0, 0, 0, 0, tzTaipei),
		},
		{
			name:      "nil effectiveFrom defaults to now",
			fromStr:   nil,
			untilStr:  strPtr("2026-12-31"),
			wantFrom:  fixedNow,
			wantUntil: time.Date(2027, 1, 1, 0, 0, 0, 0, tzTaipei),
		},
		{
			name:      "expiresAt == today (Taipei) is valid, until = tomorrow 00:00 Taipei",
			fromStr:   nil,
			untilStr:  strPtr(todayTaipei.Format("2006-01-02")),
			wantFrom:  fixedNow,
			wantUntil: time.Date(todayTaipei.Year(), todayTaipei.Month(), todayTaipei.Day()+1, 0, 0, 0, 0, tzTaipei),
		},
		{
			name:     "nil expiresAt is rejected — required on a grant",
			fromStr:  nil,
			untilStr: nil,
			wantErr:  true,
		},
		{
			name:     "malformed expiresAt",
			fromStr:  nil,
			untilStr: strPtr("31-12-2026"),
			wantErr:  true,
		},
		{
			name:     "malformed effectiveFrom",
			fromStr:  strPtr("not-a-date"),
			untilStr: strPtr("2026-12-31"),
			wantErr:  true,
		},
		{
			name:     "from after until is rejected",
			fromStr:  strPtr("2026-12-31"),
			untilStr: strPtr("2026-09-01"),
			wantErr:  true,
		},
		{
			// Regression for the from==until off-by-one: until is always shifted
			// +1 day from untilStr, so effectiveFrom one day past expiresAt lands
			// exactly ON until. A strict from.After(until) check misses this
			// (equal is not "after"); the fix must reject on !from.Before(until).
			name:     "effectiveFrom exactly one day after expiresAt is rejected (from == until)",
			fromStr:  strPtr("2026-09-02"),
			untilStr: strPtr("2026-09-01"),
			wantErr:  true,
		},
		{
			name:      "effectiveFrom == expiresAt (same civil day) is accepted as a one-day window",
			fromStr:   strPtr("2026-09-01"),
			untilStr:  strPtr("2026-09-01"),
			wantFrom:  time.Date(2026, 9, 1, 0, 0, 0, 0, tzTaipei),
			wantUntil: time.Date(2026, 9, 2, 0, 0, 0, 0, tzTaipei),
		},
		{
			name:     "expiresAt in the past is rejected",
			fromStr:  nil,
			untilStr: strPtr("2020-01-01"),
			wantErr:  true,
		},
		{
			// Distinct from "from after until is rejected": here explicit
			// effectiveFrom precedes expiresAt (so the from-after-until check
			// passes), isolating the until-not-in-the-past check as the one
			// that must fire.
			name:     "both dates in the past, from before until — still rejected as expired",
			fromStr:  strPtr("2019-01-01"),
			untilStr: strPtr("2019-06-01"),
			wantErr:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			from, until, err := parseWindow(tc.fromStr, tc.untilStr, fixedNow)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.True(t, tc.wantFrom.Equal(from), "from: got %v want %v", from, tc.wantFrom)
			assert.True(t, tc.wantUntil.Equal(until), "until: got %v want %v", until, tc.wantUntil)
		})
	}
}

func TestDisplayDate(t *testing.T) {
	got := displayDate(time.Date(2026, 9, 1, 0, 0, 0, 0, tzTaipei))
	assert.Equal(t, "2026-09-01", got)
}

func TestDisplayUntilDate(t *testing.T) {
	tests := []struct {
		name  string
		input time.Time
		want  string
	}{
		{
			name:  "round-trip: 2026-12-31 request date -> stored until-instant -> displayed back as 2026-12-31",
			input: time.Date(2027, 1, 1, 0, 0, 0, 0, tzTaipei),
			want:  "2026-12-31",
		},
		{
			name:  "civil +1 day across a month boundary",
			input: time.Date(2026, 10, 1, 0, 0, 0, 0, tzTaipei),
			want:  "2026-09-30",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, displayUntilDate(tc.input))
		})
	}
}

// -------------------------------------------------------------------------
// createPermissions — one subtest per spec §4.4 validation rule + success paths
// -------------------------------------------------------------------------

func TestHandler_createPermissions(t *testing.T) {
	knownPermission := string(model.PermissionExternalImageView)

	tests := []struct {
		name       string
		body       map[string]any
		setupMock  func(m *MockAdminStore)
		wantStatus int
		wantReason string
		checkBody  func(t *testing.T, body map[string]any)
	}{
		{
			name: "unknown permission → 400 unknown_permission",
			body: map[string]any{
				"permission": "bogus.permission", "subjectAccounts": []string{"alice"}, "granted": true,
				"effectiveFrom": "2026-09-01", "expiresAt": "2026-12-31",
				"applicantAccount": "carol", "approverAccount": "dave", "reason": "reason text",
			},
			setupMock:  func(m *MockAdminStore) {},
			wantStatus: http.StatusBadRequest,
			wantReason: string(errcode.PermissionUnknownKey),
		},
		{
			name: "empty subjectAccounts → 400 invalid_subject_count",
			body: map[string]any{
				"permission": knownPermission, "subjectAccounts": []string{}, "granted": true,
				"effectiveFrom": "2026-09-01", "expiresAt": "2026-12-31",
				"applicantAccount": "carol", "approverAccount": "dave", "reason": "reason text",
			},
			setupMock:  func(m *MockAdminStore) {},
			wantStatus: http.StatusBadRequest,
			wantReason: string(errcode.PermissionInvalidSubjects),
		},
		{
			name: "201 subjectAccounts → 400 invalid_subject_count",
			body: map[string]any{
				"permission": knownPermission, "subjectAccounts": manyAccounts(201), "granted": true,
				"effectiveFrom": "2026-09-01", "expiresAt": "2026-12-31",
				"applicantAccount": "carol", "approverAccount": "dave", "reason": "reason text",
			},
			setupMock:  func(m *MockAdminStore) {},
			wantStatus: http.StatusBadRequest,
			wantReason: string(errcode.PermissionInvalidSubjects),
		},
		{
			name: "empty reason → 201, stored as empty string",
			body: map[string]any{
				"permission": knownPermission, "subjectAccounts": []string{"alice"}, "granted": true,
				"effectiveFrom": "2026-09-01", "expiresAt": "2026-12-31",
				"applicantAccount": "carol", "approverAccount": "dave", "reason": "",
			},
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().FindAccountStates(gomock.Any(), "site-A", gomock.Any()).
					Return(map[string]bool{"alice": true, "carol": true, "dave": true}, nil)
				m.EXPECT().InsertPermissionGrants(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, grants []*model.PermissionGrant) error {
						require.Len(t, grants, 1)
						assert.Equal(t, "", grants[0].Reason)
						return nil
					})
				m.EXPECT().AppendAuditMany(gomock.Any(), gomock.Any()).Return(nil)
			},
			wantStatus: http.StatusCreated,
		},
		{
			name: "reason absent from body → 201, stored as empty string",
			body: map[string]any{
				"permission": knownPermission, "subjectAccounts": []string{"alice"}, "granted": true,
				"effectiveFrom": "2026-09-01", "expiresAt": "2026-12-31",
				"applicantAccount": "carol", "approverAccount": "dave",
				// "reason" key intentionally omitted — must behave the same as "".
			},
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().FindAccountStates(gomock.Any(), "site-A", gomock.Any()).
					Return(map[string]bool{"alice": true, "carol": true, "dave": true}, nil)
				m.EXPECT().InsertPermissionGrants(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, grants []*model.PermissionGrant) error {
						require.Len(t, grants, 1)
						assert.Equal(t, "", grants[0].Reason)
						return nil
					})
				m.EXPECT().AppendAuditMany(gomock.Any(), gomock.Any()).Return(nil)
			},
			wantStatus: http.StatusCreated,
		},
		{
			name: "reason over 1000 runes → 400 invalid_reason",
			body: map[string]any{
				"permission": knownPermission, "subjectAccounts": []string{"alice"}, "granted": true,
				"effectiveFrom": "2026-09-01", "expiresAt": "2026-12-31",
				"applicantAccount": "carol", "approverAccount": "dave", "reason": strings.Repeat("測", 1001),
			},
			setupMock:  func(m *MockAdminStore) {},
			wantStatus: http.StatusBadRequest,
			wantReason: string(errcode.PermissionInvalidReason),
		},
		{
			name: "missing applicantAccount → 400 missing_permission_fields",
			body: map[string]any{
				"permission": knownPermission, "subjectAccounts": []string{"alice"}, "granted": true,
				"effectiveFrom": "2026-09-01", "expiresAt": "2026-12-31",
				"applicantAccount": "", "approverAccount": "dave", "reason": "reason text",
			},
			setupMock:  func(m *MockAdminStore) {},
			wantStatus: http.StatusBadRequest,
			wantReason: string(errcode.PermissionMissingFields),
		},
		{
			name: "missing approverAccount → 400 missing_permission_fields",
			body: map[string]any{
				"permission": knownPermission, "subjectAccounts": []string{"alice"}, "granted": true,
				"effectiveFrom": "2026-09-01", "expiresAt": "2026-12-31",
				"applicantAccount": "carol", "approverAccount": "", "reason": "reason text",
			},
			setupMock:  func(m *MockAdminStore) {},
			wantStatus: http.StatusBadRequest,
			wantReason: string(errcode.PermissionMissingFields),
		},
		{
			// granted has no "binding:required" tag on purpose (see permissionRequest's
			// doc comment) — an absent key must land here, in the explicit missing-fields
			// check, not fall through and silently default to false/revoke.
			name: "granted omitted → 400 missing_permission_fields",
			body: map[string]any{
				"permission": knownPermission, "subjectAccounts": []string{"alice"},
				"effectiveFrom": "2026-09-01", "expiresAt": "2026-12-31",
				"applicantAccount": "carol", "approverAccount": "dave", "reason": "reason text",
			},
			setupMock:  func(m *MockAdminStore) {},
			wantStatus: http.StatusBadRequest,
			wantReason: string(errcode.PermissionMissingFields),
		},
		{
			name: "grant missing expiresAt → 400 invalid_permission_window",
			body: map[string]any{
				"permission": knownPermission, "subjectAccounts": []string{"alice"}, "granted": true,
				"applicantAccount": "carol", "approverAccount": "dave", "reason": "reason text",
			},
			setupMock:  func(m *MockAdminStore) {},
			wantStatus: http.StatusBadRequest,
			wantReason: string(errcode.PermissionInvalidWindow),
		},
		{
			name: "malformed expiresAt → 400 invalid_permission_window",
			body: map[string]any{
				"permission": knownPermission, "subjectAccounts": []string{"alice"}, "granted": true,
				"expiresAt": "31/12/2026", "applicantAccount": "carol", "approverAccount": "dave", "reason": "reason text",
			},
			setupMock:  func(m *MockAdminStore) {},
			wantStatus: http.StatusBadRequest,
			wantReason: string(errcode.PermissionInvalidWindow),
		},
		{
			name: "effectiveFrom after expiresAt → 400 invalid_permission_window",
			body: map[string]any{
				"permission": knownPermission, "subjectAccounts": []string{"alice"}, "granted": true,
				"effectiveFrom": futureDate(2), "expiresAt": futureDate(1),
				"applicantAccount": "carol", "approverAccount": "dave", "reason": "reason text",
			},
			setupMock:  func(m *MockAdminStore) {},
			wantStatus: http.StatusBadRequest,
			wantReason: string(errcode.PermissionInvalidWindow),
		},
		{
			name: "expiresAt in the past → 400 invalid_permission_window",
			body: map[string]any{
				"permission": knownPermission, "subjectAccounts": []string{"alice"}, "granted": true,
				"expiresAt": "2020-01-01", "applicantAccount": "carol", "approverAccount": "dave", "reason": "reason text",
			},
			setupMock:  func(m *MockAdminStore) {},
			wantStatus: http.StatusBadRequest,
			wantReason: string(errcode.PermissionInvalidWindow),
			checkBody: func(t *testing.T, body map[string]any) {
				// effectiveFrom was never sent — the message must name expiresAt, not
				// misreport an effectiveFrom/expiresAt ordering problem the caller
				// never created (from defaults to now, which is > any past expiresAt).
				assert.Equal(t, "expiresAt must not be in the past", body["error"])
			},
		},
		{
			name: "revoke with effectiveFrom present → 400 unexpected_permission_window",
			body: map[string]any{
				"permission": knownPermission, "subjectAccounts": []string{"alice"}, "granted": false,
				"effectiveFrom": "2026-09-01", "applicantAccount": "carol", "approverAccount": "dave", "reason": "reason text",
			},
			setupMock:  func(m *MockAdminStore) {},
			wantStatus: http.StatusBadRequest,
			wantReason: string(errcode.PermissionUnexpectedWindow),
		},
		{
			name: "revoke with expiresAt present → 400 unexpected_permission_window",
			body: map[string]any{
				"permission": knownPermission, "subjectAccounts": []string{"alice"}, "granted": false,
				"expiresAt": "2026-12-31", "applicantAccount": "carol", "approverAccount": "dave", "reason": "reason text",
			},
			setupMock:  func(m *MockAdminStore) {},
			wantStatus: http.StatusBadRequest,
			wantReason: string(errcode.PermissionUnexpectedWindow),
		},
		{
			name: "duplicate subjects deduped and reported, only unique accounts hit the store",
			body: map[string]any{
				"permission": knownPermission, "subjectAccounts": []string{"alice", "alice", "bob"}, "granted": true,
				"effectiveFrom": "2026-09-01", "expiresAt": "2026-12-31",
				"applicantAccount": "carol", "approverAccount": "dave", "reason": "reason text",
			},
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().FindAccountStates(gomock.Any(), "site-A", gomock.Any()).
					DoAndReturn(func(_ context.Context, _ string, accounts []string) (map[string]bool, error) {
						assert.Equal(t, []string{"alice", "bob", "carol", "dave"}, accounts,
							"dedup must run before the lookup, which must also cover applicant+approver")
						return map[string]bool{"alice": true, "bob": true, "carol": true, "dave": true}, nil
					})
				m.EXPECT().InsertPermissionGrants(gomock.Any(), gomock.Any()).Return(nil)
				m.EXPECT().AppendAuditMany(gomock.Any(), gomock.Any()).Return(nil)
			},
			wantStatus: http.StatusCreated,
			checkBody: func(t *testing.T, body map[string]any) {
				assert.Equal(t, float64(2), body["created"])
				assert.Equal(t, []any{"alice"}, body["duplicatesIgnored"])
			},
		},
		{
			name: "unknown accounts → 404 unknown_accounts, message names them",
			body: map[string]any{
				"permission": knownPermission, "subjectAccounts": []string{"alice", "ghost"}, "granted": true,
				"effectiveFrom": "2026-09-01", "expiresAt": "2026-12-31",
				"applicantAccount": "carol", "approverAccount": "dave", "reason": "reason text",
			},
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().FindAccountStates(gomock.Any(), "site-A", gomock.Any()).
					Return(map[string]bool{"alice": true, "carol": true, "dave": true}, nil)
			},
			wantStatus: http.StatusNotFound,
			wantReason: string(errcode.PermissionUnknownAccounts),
			checkBody: func(t *testing.T, body map[string]any) {
				assert.Contains(t, body["error"], "ghost")
				md, _ := body["metadata"].(map[string]any)
				assert.Equal(t, "ghost", md["accounts"]) // console renders metadata.accounts
			},
		},
		{
			name: "unknown applicant → 404 unknown_accounts",
			body: map[string]any{
				"permission": knownPermission, "subjectAccounts": []string{"alice"}, "granted": true,
				"effectiveFrom": "2026-09-01", "expiresAt": "2026-12-31",
				"applicantAccount": "ghost-applicant", "approverAccount": "dave", "reason": "reason text",
			},
			setupMock: func(m *MockAdminStore) {
				// alice and dave exist; ghost-applicant does not — the existence check
				// must reach applicantAccount, not just subjectAccounts (design §4.4 step 10).
				m.EXPECT().FindAccountStates(gomock.Any(), "site-A", gomock.Any()).
					Return(map[string]bool{"alice": true, "dave": true}, nil)
			},
			wantStatus: http.StatusNotFound,
			wantReason: string(errcode.PermissionUnknownAccounts),
			checkBody: func(t *testing.T, body map[string]any) {
				assert.Contains(t, body["error"], "ghost-applicant")
				md, _ := body["metadata"].(map[string]any)
				assert.Equal(t, "ghost-applicant", md["accounts"])
			},
		},
		{
			name: "unknown approver → 404 unknown_accounts",
			body: map[string]any{
				"permission": knownPermission, "subjectAccounts": []string{"alice"}, "granted": true,
				"effectiveFrom": "2026-09-01", "expiresAt": "2026-12-31",
				"applicantAccount": "carol", "approverAccount": "ghost-approver", "reason": "reason text",
			},
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().FindAccountStates(gomock.Any(), "site-A", gomock.Any()).
					Return(map[string]bool{"alice": true, "carol": true}, nil)
			},
			wantStatus: http.StatusNotFound,
			wantReason: string(errcode.PermissionUnknownAccounts),
			checkBody: func(t *testing.T, body map[string]any) {
				assert.Contains(t, body["error"], "ghost-approver")
				md, _ := body["metadata"].(map[string]any)
				assert.Equal(t, "ghost-approver", md["accounts"])
			},
		},
		{
			name: "inactive subject → 400 inactive_subject, message names it",
			body: map[string]any{
				"permission": knownPermission, "subjectAccounts": []string{"alice"}, "granted": true,
				"effectiveFrom": "2026-09-01", "expiresAt": "2026-12-31",
				"applicantAccount": "carol", "approverAccount": "dave", "reason": "reason text",
			},
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().FindAccountStates(gomock.Any(), "site-A", gomock.Any()).
					Return(map[string]bool{"alice": false, "carol": true, "dave": true}, nil)
			},
			wantStatus: http.StatusBadRequest,
			wantReason: string(errcode.PermissionInactiveSubject),
			checkBody: func(t *testing.T, body map[string]any) {
				assert.Contains(t, body["error"], "alice")
				md, _ := body["metadata"].(map[string]any)
				assert.Equal(t, "alice", md["accounts"]) // console renders metadata.accounts
			},
		},
		{
			// Design §4.4 step 10: an inactive applicant/approver is explicitly
			// allowed — only subjects are checked for IsActive(). This locks that
			// the inactive-subject rejection above does NOT also apply to applicant/approver.
			name: "inactive applicant (exists, active=false) with valid active subjects → 201 success",
			body: map[string]any{
				"permission": knownPermission, "subjectAccounts": []string{"alice"}, "granted": true,
				"effectiveFrom": "2026-09-01", "expiresAt": "2026-12-31",
				"applicantAccount": "inactive-carol", "approverAccount": "dave", "reason": "reason text",
			},
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().FindAccountStates(gomock.Any(), "site-A", gomock.Any()).
					Return(map[string]bool{"alice": true, "inactive-carol": false, "dave": true}, nil)
				m.EXPECT().InsertPermissionGrants(gomock.Any(), gomock.Any()).Return(nil)
				m.EXPECT().AppendAuditMany(gomock.Any(), gomock.Any()).Return(nil)
			},
			wantStatus: http.StatusCreated,
			checkBody: func(t *testing.T, body map[string]any) {
				assert.Equal(t, float64(1), body["created"])
			},
		},
		{
			name: "FindAccountStates store error → 500",
			body: map[string]any{
				"permission": knownPermission, "subjectAccounts": []string{"alice"}, "granted": true,
				"effectiveFrom": "2026-09-01", "expiresAt": "2026-12-31",
				"applicantAccount": "carol", "approverAccount": "dave", "reason": "reason text",
			},
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().FindAccountStates(gomock.Any(), "site-A", gomock.Any()).
					Return(nil, fmt.Errorf("boom"))
			},
			wantStatus: http.StatusInternalServerError,
		},
		{
			name: "store insert error → 500, no audit call",
			body: map[string]any{
				"permission": knownPermission, "subjectAccounts": []string{"alice"}, "granted": true,
				"effectiveFrom": "2026-09-01", "expiresAt": "2026-12-31",
				"applicantAccount": "carol", "approverAccount": "dave", "reason": "reason text",
			},
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().FindAccountStates(gomock.Any(), "site-A", gomock.Any()).
					Return(map[string]bool{"alice": true, "carol": true, "dave": true}, nil)
				m.EXPECT().InsertPermissionGrants(gomock.Any(), gomock.Any()).
					Return(fmt.Errorf("boom"))
				m.EXPECT().AppendAuditMany(gomock.Any(), gomock.Any()).Times(0)
			},
			wantStatus: http.StatusInternalServerError,
		},
		{
			name: "success grant: 201, store receives the derived instants, audit entries match",
			body: map[string]any{
				"permission": knownPermission, "subjectAccounts": []string{"alice", "bob"}, "granted": true,
				"effectiveFrom": "2026-09-01", "expiresAt": "2026-12-31",
				"applicantAccount": "carol", "approverAccount": "dave", "reason": "reason text",
			},
			setupMock: func(m *MockAdminStore) {
				wantFrom := time.Date(2026, 9, 1, 0, 0, 0, 0, tzTaipei)
				wantUntil := time.Date(2027, 1, 1, 0, 0, 0, 0, tzTaipei)

				m.EXPECT().FindAccountStates(gomock.Any(), "site-A", gomock.Any()).
					Return(map[string]bool{"alice": true, "bob": true, "carol": true, "dave": true}, nil)
				m.EXPECT().InsertPermissionGrants(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, grants []*model.PermissionGrant) error {
						require.Len(t, grants, 2)
						for _, g := range grants {
							assert.Equal(t, "site-A", g.SiteID)
							assert.Equal(t, model.PermissionExternalImageView, g.Permission)
							assert.True(t, g.Granted)
							require.NotNil(t, g.EffectiveFrom)
							require.NotNil(t, g.ExpiresAt)
							assert.True(t, wantFrom.Equal(*g.EffectiveFrom), "effectiveFrom: got %v want %v", *g.EffectiveFrom, wantFrom)
							assert.True(t, wantUntil.Equal(*g.ExpiresAt), "expiresAt: got %v want %v", *g.ExpiresAt, wantUntil)
							assert.Equal(t, "carol", g.ApplicantAccount)
							assert.Equal(t, "dave", g.ApproverAccount)
							assert.Equal(t, "p_admin", g.RecordedBy)
							assert.NotEmpty(t, g.ID)
						}
						return nil
					})
				m.EXPECT().AppendAuditMany(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, entries []*AuditEntry) error {
						require.Len(t, entries, 2)
						for _, e := range entries {
							assert.Equal(t, auditActionPermissionGrant, e.Action)
							assert.Equal(t, map[string]string{"permission": knownPermission}, e.Details)
							assert.Equal(t, "p_admin", e.ActorAccount)
						}
						return nil
					})
			},
			wantStatus: http.StatusCreated,
			checkBody: func(t *testing.T, body map[string]any) {
				assert.Equal(t, float64(2), body["created"])
				assert.Equal(t, []any{}, body["duplicatesIgnored"])
				grants, ok := body["grants"].([]any)
				require.True(t, ok)
				assert.Len(t, grants, 2)
			},
		},
		{
			name: "success revoke: stored rows carry nil window pointers",
			body: map[string]any{
				"permission": knownPermission, "subjectAccounts": []string{"alice"}, "granted": false,
				"applicantAccount": "carol", "approverAccount": "dave", "reason": "project ended",
			},
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().FindAccountStates(gomock.Any(), "site-A", gomock.Any()).
					Return(map[string]bool{"alice": true, "carol": true, "dave": true}, nil)
				m.EXPECT().InsertPermissionGrants(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, grants []*model.PermissionGrant) error {
						require.Len(t, grants, 1)
						assert.False(t, grants[0].Granted)
						assert.Nil(t, grants[0].EffectiveFrom)
						assert.Nil(t, grants[0].ExpiresAt)
						return nil
					})
				m.EXPECT().AppendAuditMany(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, entries []*AuditEntry) error {
						require.Len(t, entries, 1)
						assert.Equal(t, auditActionPermissionRevoke, entries[0].Action)
						return nil
					})
			},
			wantStatus: http.StatusCreated,
		},
		{
			// Best-effort audit contract: AppendAuditMany failing must never fail
			// the request — the ledger write already committed. Locks the current
			// slog.ErrorContext-and-continue behavior against regression.
			name: "AppendAuditMany store error → still 201 (best-effort audit)",
			body: map[string]any{
				"permission": knownPermission, "subjectAccounts": []string{"alice"}, "granted": true,
				"effectiveFrom": "2026-09-01", "expiresAt": "2026-12-31",
				"applicantAccount": "carol", "approverAccount": "dave", "reason": "reason text",
			},
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().FindAccountStates(gomock.Any(), "site-A", gomock.Any()).
					Return(map[string]bool{"alice": true, "carol": true, "dave": true}, nil)
				m.EXPECT().InsertPermissionGrants(gomock.Any(), gomock.Any()).Return(nil)
				m.EXPECT().AppendAuditMany(gomock.Any(), gomock.Any()).Return(fmt.Errorf("boom"))
			},
			wantStatus: http.StatusCreated,
		},
		{
			name: "exactly 200 subjects (the max) → 201",
			body: map[string]any{
				"permission": knownPermission, "subjectAccounts": manyAccounts(model.MaxSubjects), "granted": true,
				"effectiveFrom": "2026-09-01", "expiresAt": "2026-12-31",
				"applicantAccount": "carol", "approverAccount": "dave", "reason": "reason text",
			},
			setupMock: func(m *MockAdminStore) {
				states := make(map[string]bool, model.MaxSubjects+2)
				for _, a := range manyAccounts(model.MaxSubjects) {
					states[a] = true
				}
				states["carol"] = true
				states["dave"] = true
				m.EXPECT().FindAccountStates(gomock.Any(), "site-A", gomock.Any()).Return(states, nil)
				m.EXPECT().InsertPermissionGrants(gomock.Any(), gomock.Any()).Return(nil)
				m.EXPECT().AppendAuditMany(gomock.Any(), gomock.Any()).Return(nil)
			},
			wantStatus: http.StatusCreated,
			checkBody: func(t *testing.T, body map[string]any) {
				assert.Equal(t, float64(model.MaxSubjects), body["created"])
			},
		},
		{
			name: "exactly 1000-rune reason (the max) → 201",
			body: map[string]any{
				"permission": knownPermission, "subjectAccounts": []string{"alice"}, "granted": true,
				"effectiveFrom": "2026-09-01", "expiresAt": "2026-12-31",
				"applicantAccount": "carol", "approverAccount": "dave", "reason": strings.Repeat("測", model.MaxReasonRunes),
			},
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().FindAccountStates(gomock.Any(), "site-A", gomock.Any()).
					Return(map[string]bool{"alice": true, "carol": true, "dave": true}, nil)
				m.EXPECT().InsertPermissionGrants(gomock.Any(), gomock.Any()).Return(nil)
				m.EXPECT().AppendAuditMany(gomock.Any(), gomock.Any()).Return(nil)
			},
			wantStatus: http.StatusCreated,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			m := NewMockAdminStore(ctrl)
			tc.setupMock(m)

			h := newHandler(m, emptySessionStore(), testCfg(), nil)
			r := setupPermissionsRouter(h)

			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/permissions", bodyBytes(t, tc.body))
			req.Header.Set("Content-Type", "application/json")
			r.ServeHTTP(w, req)

			assert.Equal(t, tc.wantStatus, w.Code)
			if tc.wantReason != "" {
				body := respBody(t, w)
				assert.Equal(t, tc.wantReason, body["reason"])
			}
			if tc.checkBody != nil {
				body := respBody(t, w)
				tc.checkBody(t, body)
			}
		})
	}
}

// -------------------------------------------------------------------------
// listPermissions
// -------------------------------------------------------------------------

// timePtr returns a pointer to t, for constructing model.PermissionGrant
// time-window fixtures. integration_test.go shares it — that file compiles
// into the same package under -tags integration, while this untagged file is
// part of every build.
func timePtr(t time.Time) *time.Time { return &t }

func TestHandler_listPermissions(t *testing.T) {
	knownPermission := string(model.PermissionExternalImageView)

	tests := []struct {
		name       string
		query      string
		setupMock  func(m *MockAdminStore)
		wantStatus int
		wantReason string
		checkBody  func(t *testing.T, body map[string]any)
	}{
		{
			name:  "no filters → 200, all rows for the site passed through, no currentlyGranted",
			query: "",
			setupMock: func(m *MockAdminStore) {
				rows := []model.PermissionGrant{
					{ID: "g1", SiteID: "site-A", Permission: model.PermissionExternalImageView, SubjectAccount: "alice", Granted: true, ApplicantAccount: "carol", ApproverAccount: "dave", Reason: "r1", RecordedBy: "p_admin", RecordedAt: time.Now().UTC()},
					{ID: "g2", SiteID: "site-A", Permission: model.PermissionKey("other.permission"), SubjectAccount: "bob", Granted: false, ApplicantAccount: "carol", ApproverAccount: "dave", Reason: "r2", RecordedBy: "p_admin", RecordedAt: time.Now().UTC()},
				}
				m.EXPECT().ListPermissionGrants(gomock.Any(), "site-A", "", model.PermissionKey(""), 1, 20).
					Return(rows, int64(2), nil)
				m.EXPECT().GetLatestPermissionGrant(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
			},
			wantStatus: http.StatusOK,
			checkBody: func(t *testing.T, body map[string]any) {
				entries, ok := body["entries"].([]any)
				require.True(t, ok)
				assert.Len(t, entries, 2)
				assert.Equal(t, float64(2), body["total"])
				_, present := body["currentlyGranted"]
				assert.False(t, present, "no filters → no currentlyGranted key")
			},
		},
		{
			name:  "permission only (no subjectAccount) → 200, no currentlyGranted, mock receives subjectAccount \"\"",
			query: "?permission=" + knownPermission,
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().ListPermissionGrants(gomock.Any(), "site-A", "", model.PermissionExternalImageView, 1, 20).
					Return([]model.PermissionGrant{}, int64(0), nil)
				m.EXPECT().GetLatestPermissionGrant(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
			},
			wantStatus: http.StatusOK,
			checkBody: func(t *testing.T, body map[string]any) {
				assert.Equal(t, []any{}, body["entries"])
				_, present := body["currentlyGranted"]
				assert.False(t, present, "subjectAccount omitted → no currentlyGranted key even though permission was given")
			},
		},
		{
			name:       "unknown permission → 400 unknown_permission",
			query:      "?subjectAccount=alice&permission=bogus.permission",
			setupMock:  func(m *MockAdminStore) {},
			wantStatus: http.StatusBadRequest,
			wantReason: string(errcode.PermissionUnknownKey),
		},
		{
			name:  "store error → 500",
			query: "?subjectAccount=alice",
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().ListPermissionGrants(gomock.Any(), "site-A", "alice", model.PermissionKey(""), 1, 20).
					Return(nil, int64(0), fmt.Errorf("boom"))
			},
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:  "GetLatestPermissionGrant store error → 500",
			query: "?subjectAccount=alice&permission=" + knownPermission,
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().ListPermissionGrants(gomock.Any(), "site-A", "alice", model.PermissionExternalImageView, 1, 20).
					Return([]model.PermissionGrant{}, int64(0), nil)
				m.EXPECT().GetLatestPermissionGrant(gomock.Any(), "site-A", model.PermissionExternalImageView, "alice").
					Return(nil, fmt.Errorf("boom"))
			},
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:  "subjectAccount only, permission omitted → 200, no currentlyGranted key",
			query: "?subjectAccount=alice",
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().ListPermissionGrants(gomock.Any(), "site-A", "alice", model.PermissionKey(""), 1, 20).
					Return([]model.PermissionGrant{}, int64(0), nil)
				m.EXPECT().GetLatestPermissionGrant(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
			},
			wantStatus: http.StatusOK,
			checkBody: func(t *testing.T, body map[string]any) {
				assert.Equal(t, []any{}, body["entries"])
				assert.Equal(t, float64(0), body["total"])
				_, present := body["currentlyGranted"]
				assert.False(t, present, "permission omitted → no currentlyGranted key")
			},
		},
		{
			name:  "empty ledger WITH a permission param — currentlyGranted present, false",
			query: "?subjectAccount=alice&permission=" + knownPermission,
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().ListPermissionGrants(gomock.Any(), "site-A", "alice", model.PermissionExternalImageView, 1, 20).
					Return([]model.PermissionGrant{}, int64(0), nil)
				m.EXPECT().GetLatestPermissionGrant(gomock.Any(), "site-A", model.PermissionExternalImageView, "alice").
					Return(nil, nil)
			},
			wantStatus: http.StatusOK,
			checkBody: func(t *testing.T, body map[string]any) {
				assert.Equal(t, []any{}, body["entries"])
				val, ok := body["currentlyGranted"]
				require.True(t, ok, "permission given → currentlyGranted key must be present")
				assert.Equal(t, false, val)
			},
		},
		{
			name:  "currently granted true",
			query: "?subjectAccount=alice&permission=" + knownPermission,
			setupMock: func(m *MockAdminStore) {
				now := time.Now().UTC()
				latest := &model.PermissionGrant{
					ID: "g1", SiteID: "site-A", Permission: model.PermissionExternalImageView,
					SubjectAccount: "alice", Granted: true,
					EffectiveFrom: timePtr(now.Add(-time.Hour)), ExpiresAt: timePtr(now.Add(time.Hour)),
					ApplicantAccount: "carol", ApproverAccount: "dave", Reason: "r", RecordedBy: "p_admin", RecordedAt: now.Add(-time.Hour),
				}
				m.EXPECT().ListPermissionGrants(gomock.Any(), "site-A", "alice", model.PermissionExternalImageView, 1, 20).
					Return([]model.PermissionGrant{*latest}, int64(1), nil)
				m.EXPECT().GetLatestPermissionGrant(gomock.Any(), "site-A", model.PermissionExternalImageView, "alice").
					Return(latest, nil)
			},
			wantStatus: http.StatusOK,
			checkBody: func(t *testing.T, body map[string]any) {
				assert.Equal(t, true, body["currentlyGranted"])
			},
		},
		{
			name:  "ledger with a revoke row and a grant row: shapes differ",
			query: "?subjectAccount=alice",
			setupMock: func(m *MockAdminStore) {
				grantRow := model.PermissionGrant{
					ID: "g-grant", SiteID: "site-A", Permission: model.PermissionExternalImageView,
					SubjectAccount: "alice", Granted: true,
					EffectiveFrom:    timePtr(time.Date(2026, 9, 1, 0, 0, 0, 0, tzTaipei)),
					ExpiresAt:        timePtr(time.Date(2027, 1, 1, 0, 0, 0, 0, tzTaipei)),
					ApplicantAccount: "carol", ApproverAccount: "dave", Reason: "r1",
					RecordedBy: "p_admin", RecordedAt: time.Date(2026, 9, 1, 1, 0, 0, 0, time.UTC),
				}
				revokeRow := model.PermissionGrant{
					ID: "g-revoke", SiteID: "site-A", Permission: model.PermissionExternalImageView,
					SubjectAccount: "alice", Granted: false,
					ApplicantAccount: "carol", ApproverAccount: "dave", Reason: "r2",
					RecordedBy: "p_admin", RecordedAt: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC),
				}
				m.EXPECT().ListPermissionGrants(gomock.Any(), "site-A", "alice", model.PermissionKey(""), 1, 20).
					Return([]model.PermissionGrant{revokeRow, grantRow}, int64(2), nil)
				m.EXPECT().GetLatestPermissionGrant(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
			},
			wantStatus: http.StatusOK,
			checkBody: func(t *testing.T, body map[string]any) {
				entries, ok := body["entries"].([]any)
				require.True(t, ok)
				require.Len(t, entries, 2)

				revoke, ok := entries[0].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "g-revoke", revoke["id"])
				_, hasFrom := revoke["effectiveFrom"]
				_, hasUntil := revoke["expiresAt"]
				_, hasUntilUTC := revoke["expiresAtUTC"]
				assert.False(t, hasFrom, "revoke row must not carry an effectiveFrom key")
				assert.False(t, hasUntil, "revoke row must not carry an expiresAt key")
				assert.False(t, hasUntilUTC, "revoke row must not carry an expiresAtUTC key")

				grant, ok := entries[1].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "g-grant", grant["id"])
				assert.Equal(t, "2026-09-01", grant["effectiveFrom"])
				assert.Equal(t, "2026-12-31", grant["expiresAt"])
				assert.NotEmpty(t, grant["expiresAtUTC"])
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			m := NewMockAdminStore(ctrl)
			tc.setupMock(m)

			h := newHandler(m, emptySessionStore(), testCfg(), nil)
			r := setupPermissionsRouter(h)

			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/permissions"+tc.query, nil)
			r.ServeHTTP(w, req)

			assert.Equal(t, tc.wantStatus, w.Code)
			body := respBody(t, w)
			if tc.wantReason != "" {
				assert.Equal(t, tc.wantReason, body["reason"])
			}
			if tc.checkBody != nil {
				tc.checkBody(t, body)
			}
		})
	}
}
