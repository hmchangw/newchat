package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/session"
	"github.com/hmchangw/chat/pkg/subject"
)

// setupPermissionsRouter wires h into a Gin engine with the same fake
// requireAdmin principal injection as setupRouter (handler_test.go), wired to
// only the permission routes.
func setupPermissionsRouter(h *Handler) *gin.Engine {
	r := gin.New()
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
	r.POST("/permissions/resync", h.resyncPermissions)
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
			name: "empty reason → 201, stored as empty string",
			body: map[string]any{
				"permission": knownPermission, "subjectAccounts": []string{"alice"}, "granted": true,
				"effectiveFrom": "2026-09-01", "expiresAt": "2026-12-31",
				"applicantAccount": "carol", "approverAccount": "dave", "reason": "",
			},
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().FindAccountStates(gomock.Any(), gomock.Any()).
					Return(map[string]bool{"alice": true, "carol": true, "dave": true}, nil)
				m.EXPECT().RecordPermissionChange(gomock.Any(), gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, grants []*model.PermissionGrant, _ model.PermissionState) error {
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
				m.EXPECT().FindAccountStates(gomock.Any(), gomock.Any()).
					Return(map[string]bool{"alice": true, "carol": true, "dave": true}, nil)
				m.EXPECT().RecordPermissionChange(gomock.Any(), gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, grants []*model.PermissionGrant, _ model.PermissionState) error {
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
				m.EXPECT().FindAccountStates(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, accounts []string) (map[string]bool, error) {
						assert.Equal(t, []string{"alice", "bob", "carol", "dave"}, accounts,
							"dedup must run before the lookup, which must also cover applicant+approver")
						return map[string]bool{"alice": true, "bob": true, "carol": true, "dave": true}, nil
					})
				m.EXPECT().RecordPermissionChange(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
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
				m.EXPECT().FindAccountStates(gomock.Any(), gomock.Any()).
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
				m.EXPECT().FindAccountStates(gomock.Any(), gomock.Any()).
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
				m.EXPECT().FindAccountStates(gomock.Any(), gomock.Any()).
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
				m.EXPECT().FindAccountStates(gomock.Any(), gomock.Any()).
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
				m.EXPECT().FindAccountStates(gomock.Any(), gomock.Any()).
					Return(map[string]bool{"alice": true, "inactive-carol": false, "dave": true}, nil)
				m.EXPECT().RecordPermissionChange(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
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
				m.EXPECT().FindAccountStates(gomock.Any(), gomock.Any()).
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
				m.EXPECT().FindAccountStates(gomock.Any(), gomock.Any()).
					Return(map[string]bool{"alice": true, "carol": true, "dave": true}, nil)
				m.EXPECT().RecordPermissionChange(gomock.Any(), gomock.Any(), gomock.Any()).
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

				m.EXPECT().FindAccountStates(gomock.Any(), gomock.Any()).
					Return(map[string]bool{"alice": true, "bob": true, "carol": true, "dave": true}, nil)
				m.EXPECT().RecordPermissionChange(gomock.Any(), gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, grants []*model.PermissionGrant, state model.PermissionState) error {
						require.Len(t, grants, 2)
						for _, g := range grants {
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
						assert.True(t, state.Granted)
						require.NotNil(t, state.EffectiveFrom)
						require.NotNil(t, state.ExpiresAt)
						assert.True(t, wantFrom.Equal(*state.EffectiveFrom), "state.EffectiveFrom: got %v want %v", *state.EffectiveFrom, wantFrom)
						assert.True(t, wantUntil.Equal(*state.ExpiresAt), "state.ExpiresAt: got %v want %v", *state.ExpiresAt, wantUntil)
						assert.True(t, grants[0].RecordedAt.Equal(state.UpdatedAt), "state.UpdatedAt must equal the grants' shared RecordedAt")
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
			},
		},
		{
			name: "success revoke: stored rows carry nil window pointers",
			body: map[string]any{
				"permission": knownPermission, "subjectAccounts": []string{"alice"}, "granted": false,
				"applicantAccount": "carol", "approverAccount": "dave", "reason": "project ended",
			},
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().FindAccountStates(gomock.Any(), gomock.Any()).
					Return(map[string]bool{"alice": true, "carol": true, "dave": true}, nil)
				m.EXPECT().RecordPermissionChange(gomock.Any(), gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, grants []*model.PermissionGrant, state model.PermissionState) error {
						require.Len(t, grants, 1)
						assert.False(t, grants[0].Granted)
						assert.Nil(t, grants[0].EffectiveFrom)
						assert.Nil(t, grants[0].ExpiresAt)
						assert.False(t, state.Granted)
						assert.Nil(t, state.EffectiveFrom)
						assert.Nil(t, state.ExpiresAt)
						assert.True(t, grants[0].RecordedAt.Equal(state.UpdatedAt), "state.UpdatedAt must equal the grants' shared RecordedAt")
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
				m.EXPECT().FindAccountStates(gomock.Any(), gomock.Any()).
					Return(map[string]bool{"alice": true, "carol": true, "dave": true}, nil)
				m.EXPECT().RecordPermissionChange(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
				m.EXPECT().AppendAuditMany(gomock.Any(), gomock.Any()).Return(fmt.Errorf("boom"))
			},
			wantStatus: http.StatusCreated,
		},
		{
			// No cap: the pre-bulk-round implementation rejected anything over a
			// 200-subject ceiling. Locks that a batch far past that old limit
			// still passes validation and reaches the store in one call.
			name: "10,001 subjects (no cap) → 201, all pass validation",
			body: map[string]any{
				"permission": knownPermission, "subjectAccounts": manyAccounts(10001), "granted": true,
				"effectiveFrom": "2026-09-01", "expiresAt": "2026-12-31",
				"applicantAccount": "carol", "approverAccount": "dave", "reason": "reason text",
			},
			setupMock: func(m *MockAdminStore) {
				accounts := manyAccounts(10001)
				states := make(map[string]bool, len(accounts)+2)
				for _, a := range accounts {
					states[a] = true
				}
				states["carol"] = true
				states["dave"] = true
				m.EXPECT().FindAccountStates(gomock.Any(), gomock.Any()).Return(states, nil)
				m.EXPECT().RecordPermissionChange(gomock.Any(), gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, grants []*model.PermissionGrant, _ model.PermissionState) error {
						require.Len(t, grants, 10001)
						return nil
					})
				m.EXPECT().AppendAuditMany(gomock.Any(), gomock.Any()).Return(nil)
			},
			wantStatus: http.StatusCreated,
			checkBody: func(t *testing.T, body map[string]any) {
				assert.Equal(t, float64(10001), body["created"])
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
				m.EXPECT().FindAccountStates(gomock.Any(), gomock.Any()).
					Return(map[string]bool{"alice": true, "carol": true, "dave": true}, nil)
				m.EXPECT().RecordPermissionChange(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
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

			h := newHandler(m, emptySessionStore(), testCfg(), nil, nil)
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
// createPermissions — cross-site fanout (publishPermissionFanout)
// -------------------------------------------------------------------------

// fanoutTestCfg is the 3-site fixture used by every fanout scenario below:
// site-a (this site) publishing to remotes site-b and site-c.
func fanoutTestCfg() Config {
	return Config{SiteID: "site-a", AllSiteIDs: []string{"site-a", "site-b", "site-c"}, FanoutTimeout: 5 * time.Second}
}

// fanoutRequestBody returns a valid grant request for the given subject accounts.
func fanoutRequestBody(accounts []string) map[string]any {
	return map[string]any{
		"permission": string(model.PermissionExternalImageView), "subjectAccounts": accounts, "granted": true,
		"effectiveFrom": "2026-09-01", "expiresAt": "2026-12-31",
		"applicantAccount": "carol", "approverAccount": "dave", "reason": "reason text",
	}
}

// mockPermissionStoreOK wires FindAccountStates/RecordPermissionChange/AppendAuditMany
// to succeed for the given subject accounts plus applicant/approver carol/dave.
func mockPermissionStoreOK(m *MockAdminStore, accounts []string) {
	states := make(map[string]bool, len(accounts)+2)
	for _, a := range accounts {
		states[a] = true
	}
	states["carol"] = true
	states["dave"] = true
	m.EXPECT().FindAccountStates(gomock.Any(), gomock.Any()).Return(states, nil)
	m.EXPECT().RecordPermissionChange(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	m.EXPECT().AppendAuditMany(gomock.Any(), gomock.Any()).Return(nil)
}

// publishLog captures fanout publishes. publishPermissionFanout runs one goroutine
// per destination, so every recording closure must be race-safe; subjects[i] always
// pairs with data[i] because both are appended under the same lock.
type publishLog struct {
	mu       sync.Mutex
	subjects []string
	data     [][]byte
}

func (l *publishLog) record(subj string, data []byte) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.subjects = append(l.subjects, subj)
	l.data = append(l.data, data)
}

// publish returns a publishInbox func that records every call and succeeds.
func (l *publishLog) publish(_ context.Context, subj string, data []byte) error {
	l.record(subj, data)
	return nil
}

// postPermissions POSTs body to h's /permissions route and returns the recorder.
func postPermissions(h *Handler, body map[string]any, t *testing.T) *httptest.ResponseRecorder {
	t.Helper()
	r := setupPermissionsRouter(h)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/permissions", bodyBytes(t, body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	return w
}

func TestHandler_createPermissions_Fanout(t *testing.T) {
	t.Run("happy path: 3 subjects fan out to the 2 remote sites, syncFailures omitted", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		m := NewMockAdminStore(ctrl)
		accounts := []string{"alice", "bob", "eve"}
		mockPermissionStoreOK(m, accounts)

		var log publishLog

		h := newHandler(m, emptySessionStore(), fanoutTestCfg(), nil, log.publish)
		w := postPermissions(h, fanoutRequestBody(accounts), t)
		require.Equal(t, http.StatusCreated, w.Code)

		require.Len(t, log.subjects, 2, "one publish per remote site — site-b and site-c, self excluded")
		// Destinations publish concurrently, so only the set is deterministic.
		assert.ElementsMatch(t, []string{
			subject.InboxExternal("site-b", model.InboxUserPermissionsUpdated),
			subject.InboxExternal("site-c", model.InboxUserPermissionsUpdated),
		}, log.subjects)

		var gotDests []string
		for i, data := range log.data {
			var evt model.InboxEvent
			require.NoError(t, json.Unmarshal(data, &evt))
			assert.Equal(t, model.InboxUserPermissionsUpdated, evt.Type)
			assert.Equal(t, "site-a", evt.SiteID)
			assert.Equal(t, subject.InboxExternal(evt.DestSiteID, model.InboxUserPermissionsUpdated), log.subjects[i],
				"each event must be published on its own destination's subject")
			gotDests = append(gotDests, evt.DestSiteID)

			var payload model.UserPermissionsUpdated
			require.NoError(t, json.Unmarshal(evt.Payload, &payload))
			assert.Equal(t, model.PermissionExternalImageView, payload.Permission)
			assert.ElementsMatch(t, accounts, payload.Accounts)
			assert.True(t, payload.State.Granted)
		}
		assert.ElementsMatch(t, []string{"site-b", "site-c"}, gotDests)

		body := respBody(t, w)
		_, hasSyncFailures := body["syncFailures"]
		assert.False(t, hasSyncFailures, "syncFailures must be omitted (omitempty) when every publish succeeds")
	})

	t.Run("audit entries are written before the fanout", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		m := NewMockAdminStore(ctrl)
		accounts := []string{"alice"}
		m.EXPECT().FindAccountStates(gomock.Any(), gomock.Any()).
			Return(map[string]bool{"alice": true, "carol": true, "dave": true}, nil)
		m.EXPECT().RecordPermissionChange(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

		// Destinations publish concurrently, so guard the log; only calls[0] is asserted.
		var mu sync.Mutex
		var calls []string
		record := func(what string) {
			mu.Lock()
			defer mu.Unlock()
			calls = append(calls, what)
		}
		m.EXPECT().AppendAuditMany(gomock.Any(), gomock.Any()).
			DoAndReturn(func(context.Context, []*AuditEntry) error {
				record("audit")
				return nil
			})
		publish := func(context.Context, string, []byte) error {
			record("publish")
			return nil
		}

		h := newHandler(m, emptySessionStore(), fanoutTestCfg(), nil, publish)
		require.Equal(t, http.StatusCreated, postPermissions(h, fanoutRequestBody(accounts), t).Code)

		require.NotEmpty(t, calls)
		assert.Equal(t, "audit", calls[0],
			"audit must be written before the fanout — it runs on the cancellable request ctx, so a client disconnect during the fanout's multi-second budget would otherwise kill every audit entry for a ledger write that already committed")
	})

	t.Run("fanout ctx carries its own deadline and is severed from client cancellation", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		m := NewMockAdminStore(ctrl)
		accounts := []string{"alice"}
		mockPermissionStoreOK(m, accounts)

		// Captured synchronously INSIDE the closure, at call time — the handler's own
		// `defer cancelFanout()` fires the instant it returns, so a post-hoc check made
		// after ServeHTTP returns would see Done() closed regardless of whether the
		// cancellation came from the (already-canceled) client ctx or from that ordinary
		// end-of-handler cleanup. Checking Err()/Deadline() at call time is the only timing
		// that distinguishes "inherited the client's cancellation" from "the fanout is done".
		// Guarded: destinations publish from their own goroutines, and every one of
		// them sees the same fanout ctx, so the last write wins either way.
		var mu sync.Mutex
		var callErr error
		var callDeadline time.Time
		var callHasDeadline bool
		publish := func(ctx context.Context, _ string, _ []byte) error {
			mu.Lock()
			defer mu.Unlock()
			callErr = ctx.Err()
			callDeadline, callHasDeadline = ctx.Deadline()
			return nil
		}

		h := newHandler(m, emptySessionStore(), fanoutTestCfg(), nil, publish)
		r := setupPermissionsRouter(h)

		// Simulate a client hang-up: the request ctx is already canceled before the
		// handler even runs — the fanout must not inherit that cancellation.
		reqCtx, cancelReq := context.WithCancel(context.Background())
		cancelReq()

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/permissions", bodyBytes(t, fanoutRequestBody(accounts))).WithContext(reqCtx)
		req.Header.Set("Content-Type", "application/json")
		r.ServeHTTP(w, req)

		require.Equal(t, http.StatusCreated, w.Code, "a canceled client ctx must not fail the request — the ledger write already committed")

		assert.NoError(t, callErr, "publish ctx must not already be canceled/expired at call time — it must be severed from the client's cancellation (context.WithoutCancel)")
		require.True(t, callHasDeadline, "publish ctx must carry the fanout's own deadline, not run unbounded")
		assert.WithinDuration(t, time.Now().Add(h.cfg.FanoutTimeout), callDeadline, 2*time.Second, "fanout deadline must be the configured fanout budget, not per-publish and not unbounded")
	})

	t.Run("chunking: one account over the chunk size splits into a full chunk plus one per destination", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		m := NewMockAdminStore(ctrl)
		accounts := manyAccounts(fanoutChunkSize + 1)
		mockPermissionStoreOK(m, accounts)

		var log publishLog

		h := newHandler(m, emptySessionStore(), fanoutTestCfg(), nil, log.publish)
		w := postPermissions(h, fanoutRequestBody(accounts), t)
		require.Equal(t, http.StatusCreated, w.Code)
		require.Len(t, log.data, 4, "2 chunks x 2 remote destinations")

		chunkSizesByDest := map[string][]int{}
		accountsByDest := map[string][]string{}
		var timestamps []int64
		var states []model.PermissionState
		for i, data := range log.data {
			var evt model.InboxEvent
			require.NoError(t, json.Unmarshal(data, &evt))
			var payload model.UserPermissionsUpdated
			require.NoError(t, json.Unmarshal(evt.Payload, &payload))
			assert.Equal(t, subject.InboxExternal(evt.DestSiteID, model.InboxUserPermissionsUpdated), log.subjects[i])

			chunkSizesByDest[evt.DestSiteID] = append(chunkSizesByDest[evt.DestSiteID], len(payload.Accounts))
			accountsByDest[evt.DestSiteID] = append(accountsByDest[evt.DestSiteID], payload.Accounts...)
			timestamps = append(timestamps, payload.Timestamp)
			states = append(states, payload.State)
		}

		for _, dest := range []string{"site-b", "site-c"} {
			assert.ElementsMatch(t, []int{fanoutChunkSize, 1}, chunkSizesByDest[dest], "dest %s chunk sizes", dest)
			assert.ElementsMatch(t, accounts, accountsByDest[dest], "dest %s account union must be complete, no dupes", dest)
		}
		for _, ts := range timestamps {
			assert.Equal(t, timestamps[0], ts, "every chunk shares the same publish Timestamp")
		}
		for _, s := range states {
			assert.Equal(t, states[0], s, "every chunk shares the same derived State")
		}
	})

	t.Run("failure aggregation: site-b errors, site-c still receives its chunk, status still 201", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		m := NewMockAdminStore(ctrl)
		accounts := []string{"alice", "bob", "eve"}
		mockPermissionStoreOK(m, accounts)

		var log publishLog
		publish := func(ctx context.Context, subj string, data []byte) error {
			if strings.Contains(subj, ".site-b.") {
				return fmt.Errorf("simulated publish failure")
			}
			return log.publish(ctx, subj, data)
		}

		h := newHandler(m, emptySessionStore(), fanoutTestCfg(), nil, publish)
		w := postPermissions(h, fanoutRequestBody(accounts), t)
		require.Equal(t, http.StatusCreated, w.Code, "fanout failures must never fail the request")

		require.Len(t, log.subjects, 1, "site-c's publish must still land")
		assert.Equal(t, subject.InboxExternal("site-c", model.InboxUserPermissionsUpdated), log.subjects[0])

		body := respBody(t, w)
		assert.Equal(t, []any{"site-b"}, body["syncFailures"])
	})

	t.Run("blank and self entries in AllSiteIDs are skipped, no publishes", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		m := NewMockAdminStore(ctrl)
		accounts := []string{"alice"}
		mockPermissionStoreOK(m, accounts)

		published := 0
		publish := func(_ context.Context, _ string, _ []byte) error {
			published++
			return nil
		}

		cfg := Config{SiteID: "site-a", AllSiteIDs: []string{"", "site-a"}}
		h := newHandler(m, emptySessionStore(), cfg, nil, publish)
		w := postPermissions(h, fanoutRequestBody(accounts), t)
		require.Equal(t, http.StatusCreated, w.Code)
		assert.Zero(t, published)

		body := respBody(t, w)
		_, hasSyncFailures := body["syncFailures"]
		assert.False(t, hasSyncFailures)
	})

	t.Run("store failure → zero publishes", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		m := NewMockAdminStore(ctrl)
		m.EXPECT().FindAccountStates(gomock.Any(), gomock.Any()).
			Return(map[string]bool{"alice": true, "carol": true, "dave": true}, nil)
		m.EXPECT().RecordPermissionChange(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(fmt.Errorf("boom"))

		published := 0
		publish := func(_ context.Context, _ string, _ []byte) error {
			published++
			return nil
		}

		h := newHandler(m, emptySessionStore(), fanoutTestCfg(), nil, publish)
		w := postPermissions(h, fanoutRequestBody([]string{"alice"}), t)
		require.Equal(t, http.StatusInternalServerError, w.Code)
		assert.Zero(t, published)
	})
}

func TestRemoteSites(t *testing.T) {
	tests := []struct {
		name       string
		allSiteIDs []string
		self       string
		want       []string
	}{
		{"filters out self and blank entries", []string{"", "site-a", "site-b", "site-c"}, "site-a", []string{"site-b", "site-c"}},
		{"empty AllSiteIDs → nil", nil, "site-a", nil},
		{"only self → nil", []string{"site-a"}, "site-a", nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, remoteSites(tc.allSiteIDs, tc.self))
		})
	}
}

// -------------------------------------------------------------------------
// resyncPermissions — re-delivery, not re-recording: writes nothing, fans out
// the current materialized state via publishPermissionFanout (Task 5).
// -------------------------------------------------------------------------

// postResync POSTs body to h's /permissions/resync route and returns the recorder.
func postResync(h *Handler, body map[string]any, t *testing.T) *httptest.ResponseRecorder {
	t.Helper()
	r := setupPermissionsRouter(h)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/permissions/resync", bodyBytes(t, body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	return w
}

// brokerMaxPayload is the max_payload our NATS brokers are configured with. The chunk
// size is a compile-time guess at how many accounts fit under it, so this pins the
// worst case the guess assumes — with the old 5000-account chunk this test fails.
const brokerMaxPayload = 128 << 10

func TestFanoutChunk_FitsBrokerMaxPayload(t *testing.T) {
	// Worst case: a full chunk of long accounts with every state bound set. The INBOX
	// envelope base64-encodes the inner event (InboxEvent.Payload is []byte), so the
	// bytes on the wire are ~4/3 of the event JSON — measure the envelope, not the event.
	accounts := make([]string, fanoutChunkSize)
	for i := range accounts {
		accounts[i] = fmt.Sprintf("%064d", i) // 64-char accounts, well past any real directory id
	}
	state := model.PermissionState{
		Granted:       true,
		EffectiveFrom: timePtr(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)),
		ExpiresAt:     timePtr(time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC)),
		UpdatedAt:     time.Date(2026, 6, 1, 8, 30, 0, 0, time.UTC),
	}

	var log publishLog
	h := newHandler(nil, emptySessionStore(), fanoutTestCfg(), nil, log.publish)
	failures := h.publishPermissionFanout(context.Background(), model.PermissionExternalImageView, []fanoutBatch{{accounts: accounts, state: state}})

	require.Nil(t, failures)
	require.Len(t, log.data, 2, "one full chunk -> one envelope per remote dest")
	for _, data := range log.data {
		assert.Less(t, len(data), brokerMaxPayload, "a worst-case chunk envelope must clear the broker max_payload")
	}
}

func TestHandler_resyncPermissions(t *testing.T) {
	knownPermission := string(model.PermissionExternalImageView)

	// storedState is a fixture with an UpdatedAt firmly in the past — every
	// assertion on the fanned-out event's State.UpdatedAt must see exactly
	// this value, never a freshly stamped now().
	storedState := model.PermissionState{
		Granted:       true,
		EffectiveFrom: timePtr(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)),
		ExpiresAt:     timePtr(time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC)),
		UpdatedAt:     time.Date(2025, 6, 1, 8, 30, 0, 0, time.UTC),
	}

	t.Run("unknown permission → 400 unknown_permission", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		m := NewMockAdminStore(ctrl) // no expectations — store must not be touched
		h := newHandler(m, emptySessionStore(), testCfg(), nil, nil)

		w := postResync(h, map[string]any{"permission": "bogus.permission", "accounts": []string{"alice"}}, t)

		assert.Equal(t, http.StatusBadRequest, w.Code)
		body := respBody(t, w)
		assert.Equal(t, string(errcode.PermissionUnknownKey), body["reason"])
	})

	t.Run("empty accounts → 400 invalid_subject_count", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		m := NewMockAdminStore(ctrl) // no expectations — store must not be touched
		h := newHandler(m, emptySessionStore(), testCfg(), nil, nil)

		w := postResync(h, map[string]any{"permission": knownPermission, "accounts": []string{}}, t)

		assert.Equal(t, http.StatusBadRequest, w.Code)
		body := respBody(t, w)
		assert.Equal(t, string(errcode.PermissionInvalidSubjects), body["reason"])
	})

	t.Run("GetUserPermissionsForAccounts store error → 500", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		m := NewMockAdminStore(ctrl)
		m.EXPECT().GetUserPermissionsForAccounts(gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("boom"))
		h := newHandler(m, emptySessionStore(), fanoutTestCfg(), nil, nil)

		w := postResync(h, map[string]any{"permission": knownPermission, "accounts": []string{"alice"}}, t)

		assert.Equal(t, http.StatusInternalServerError, w.Code)
	})

	t.Run("never calls RecordPermissionChange or AppendAuditMany — resync only re-delivers, never re-records", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		// No RecordPermissionChange/AppendAuditMany expectations set — gomock fails
		// the test the instant either is called.
		m := NewMockAdminStore(ctrl)
		perms := map[string]*model.UserPermissions{"alice": {ExternalImageView: &storedState}}
		m.EXPECT().GetUserPermissionsForAccounts(gomock.Any(), gomock.Any()).Return(perms, nil)
		h := newHandler(m, emptySessionStore(), fanoutTestCfg(), nil, nil)

		w := postResync(h, map[string]any{"permission": knownPermission, "accounts": []string{"alice"}}, t)

		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("no requested account has a stored state → 200, nothing published, syncFailures omitted", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		m := NewMockAdminStore(ctrl)
		perms := map[string]*model.UserPermissions{"alice": {}} // doc exists, permission never recorded; carol has no doc
		m.EXPECT().GetUserPermissionsForAccounts(gomock.Any(), gomock.Any()).Return(perms, nil)

		var log publishLog
		h := newHandler(m, emptySessionStore(), fanoutTestCfg(), nil, log.publish)
		w := postResync(h, map[string]any{"permission": knownPermission, "accounts": []string{"alice", "carol"}}, t)

		require.Equal(t, http.StatusOK, w.Code)
		assert.Empty(t, log.data, "nothing to re-deliver → no publish, even with remotes and a live publisher")
		_, has := respBody(t, w)["syncFailures"]
		assert.False(t, has, "syncFailures is omitted when no destination was attempted")
	})

	t.Run("two accounts sharing one stored state → one fanout group, one event per dest, stored UpdatedAt preserved", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		m := NewMockAdminStore(ctrl)
		perms := map[string]*model.UserPermissions{
			"alice": {ExternalImageView: &storedState},
			"bob":   {ExternalImageView: &storedState},
		}
		m.EXPECT().GetUserPermissionsForAccounts(gomock.Any(), gomock.Any()).Return(perms, nil)

		var log publishLog

		h := newHandler(m, emptySessionStore(), fanoutTestCfg(), nil, log.publish)
		w := postResync(h, map[string]any{"permission": knownPermission, "accounts": []string{"alice", "bob"}}, t)
		require.Equal(t, http.StatusOK, w.Code)

		require.Len(t, log.data, 2, "one group -> one event per remote dest (site-b, site-c)")
		// Destinations publish concurrently, so only the set is deterministic.
		assert.ElementsMatch(t, []string{
			subject.InboxExternal("site-b", model.InboxUserPermissionsUpdated),
			subject.InboxExternal("site-c", model.InboxUserPermissionsUpdated),
		}, log.subjects)

		for _, data := range log.data {
			var evt model.InboxEvent
			require.NoError(t, json.Unmarshal(data, &evt))
			var payload model.UserPermissionsUpdated
			require.NoError(t, json.Unmarshal(evt.Payload, &payload))
			assert.ElementsMatch(t, []string{"alice", "bob"}, payload.Accounts, "both accounts share the one group's event")
			assert.Equal(t, storedState.Granted, payload.State.Granted)
			require.NotNil(t, payload.State.UpdatedAt)
			assert.True(t, storedState.UpdatedAt.Equal(payload.State.UpdatedAt),
				"fanned-out State.UpdatedAt must be the STORED watermark, not a fresh timestamp: got %v want %v",
				payload.State.UpdatedAt, storedState.UpdatedAt)
		}

		body := respBody(t, w)
		_, hasSyncFailures := body["syncFailures"]
		assert.False(t, hasSyncFailures, "syncFailures omitted when every publish succeeds")
	})

	t.Run("two accounts with different stored states → two groups, two events per dest, correct account/state pairing", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		m := NewMockAdminStore(ctrl)
		stateB := model.PermissionState{
			Granted:   false,
			UpdatedAt: time.Date(2025, 7, 15, 9, 0, 0, 0, time.UTC),
		}
		perms := map[string]*model.UserPermissions{
			"alice": {ExternalImageView: &storedState},
			"bob":   {ExternalImageView: &stateB},
		}
		m.EXPECT().GetUserPermissionsForAccounts(gomock.Any(), gomock.Any()).Return(perms, nil)

		var log publishLog

		h := newHandler(m, emptySessionStore(), fanoutTestCfg(), nil, log.publish)
		w := postResync(h, map[string]any{"permission": knownPermission, "accounts": []string{"alice", "bob"}}, t)
		require.Equal(t, http.StatusOK, w.Code)

		require.Len(t, log.data, 4, "2 groups x 2 remote dests")

		gotByAccount := map[string]model.PermissionState{}
		for _, data := range log.data {
			var evt model.InboxEvent
			require.NoError(t, json.Unmarshal(data, &evt))
			var payload model.UserPermissionsUpdated
			require.NoError(t, json.Unmarshal(evt.Payload, &payload))
			require.Len(t, payload.Accounts, 1, "each group here has exactly one account")
			gotByAccount[payload.Accounts[0]] = payload.State
		}

		require.Contains(t, gotByAccount, "alice")
		require.Contains(t, gotByAccount, "bob")
		assert.Equal(t, storedState.Granted, gotByAccount["alice"].Granted)
		assert.True(t, storedState.UpdatedAt.Equal(gotByAccount["alice"].UpdatedAt))
		assert.Equal(t, stateB.Granted, gotByAccount["bob"].Granted)
		assert.True(t, stateB.UpdatedAt.Equal(gotByAccount["bob"].UpdatedAt))
	})

	t.Run("account absent from the store map and account present with Permissions == nil are both skipped", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		m := NewMockAdminStore(ctrl)
		perms := map[string]*model.UserPermissions{
			"alice":    {ExternalImageView: &storedState},
			"nullperm": nil, // present, but Permissions itself is nil
			// "ghost" intentionally absent from the map entirely
		}
		m.EXPECT().GetUserPermissionsForAccounts(gomock.Any(), gomock.Any()).Return(perms, nil)

		var log publishLog

		h := newHandler(m, emptySessionStore(), fanoutTestCfg(), nil, log.publish)
		w := postResync(h, map[string]any{"permission": knownPermission, "accounts": []string{"alice", "ghost", "nullperm"}}, t)
		require.Equal(t, http.StatusOK, w.Code)

		require.Len(t, log.data, 2, "only alice's group fans out, one event per remote dest")
		for _, data := range log.data {
			var evt model.InboxEvent
			require.NoError(t, json.Unmarshal(data, &evt))
			var payload model.UserPermissionsUpdated
			require.NoError(t, json.Unmarshal(evt.Payload, &payload))
			assert.Equal(t, []string{"alice"}, payload.Accounts)
		}
	})

	t.Run("a failing dest → syncFailures names it, 200", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		m := NewMockAdminStore(ctrl)
		perms := map[string]*model.UserPermissions{"alice": {ExternalImageView: &storedState}}
		m.EXPECT().GetUserPermissionsForAccounts(gomock.Any(), gomock.Any()).Return(perms, nil)

		publish := func(_ context.Context, subj string, _ []byte) error {
			if strings.Contains(subj, ".site-b.") {
				return fmt.Errorf("simulated publish failure")
			}
			return nil
		}

		h := newHandler(m, emptySessionStore(), fanoutTestCfg(), nil, publish)
		w := postResync(h, map[string]any{"permission": knownPermission, "accounts": []string{"alice"}}, t)
		require.Equal(t, http.StatusOK, w.Code, "fanout failures must never fail the request")

		body := respBody(t, w)
		assert.Equal(t, []any{"site-b"}, body["syncFailures"])
	})

	t.Run("every group shares ONE fanout budget — the deadline does not restart per group", func(t *testing.T) {
		// Regression pin: the budget used to be established inside publishPermissionFanout,
		// which resync called once PER state group. Groups are keyed on UpdatedAt, so N admin
		// submits yield N groups and one unreachable peer cost N × the budget in wall time —
		// past httpWriteTimeout (30s) at N ≥ 6, so net/http dropped the connection before the
		// handler could report syncFailures. One shared budget = one shared deadline instant.
		ctrl := gomock.NewController(t)
		m := NewMockAdminStore(ctrl)
		stateB := model.PermissionState{Granted: false, UpdatedAt: time.Date(2025, 7, 15, 9, 0, 0, 0, time.UTC)}
		perms := map[string]*model.UserPermissions{
			"alice": {ExternalImageView: &storedState},
			"bob":   {ExternalImageView: &stateB},
		}
		m.EXPECT().GetUserPermissionsForAccounts(gomock.Any(), gomock.Any()).Return(perms, nil)

		var mu sync.Mutex
		var deadlines []time.Time
		allHaveDeadline := true
		publish := func(ctx context.Context, _ string, _ []byte) error {
			d, ok := ctx.Deadline()
			// Recorded, not asserted, here: destinations publish from their own
			// goroutines, and require's FailNow is only legal on the test goroutine.
			mu.Lock()
			defer mu.Unlock()
			allHaveDeadline = allHaveDeadline && ok
			deadlines = append(deadlines, d)
			return nil
		}

		h := newHandler(m, emptySessionStore(), fanoutTestCfg(), nil, publish)
		w := postResync(h, map[string]any{"permission": knownPermission, "accounts": []string{"alice", "bob"}}, t)
		require.Equal(t, http.StatusOK, w.Code)

		require.Len(t, deadlines, 4, "2 groups x 2 remote dests")
		require.True(t, allHaveDeadline, "every publish ctx must carry the fanout budget's deadline")
		assert.WithinDuration(t, time.Now().Add(h.cfg.FanoutTimeout), deadlines[0], 2*time.Second, "the shared budget must be the configured fanout budget")
		for i, d := range deadlines {
			assert.True(t, d.Equal(deadlines[0]),
				"publish %d saw a different deadline (%v) than the first (%v) — each group got a fresh budget instead of sharing the request's one",
				i, d, deadlines[0])
		}
	})

	t.Run("a stalled peer consumes only its own lane — healthy peers still receive every group", func(t *testing.T) {
		// Regression pin for PR review: resync used to invoke the fanout once per state
		// group, sequentially, under the shared budget. A peer that blocked until the
		// deadline on the first group handed every later group an already-expired ctx,
		// so healthy peers missed those groups and were reported as sync failures too.
		ctrl := gomock.NewController(t)
		m := NewMockAdminStore(ctrl)
		stateB := model.PermissionState{Granted: false, UpdatedAt: time.Date(2025, 7, 15, 9, 0, 0, 0, time.UTC)}
		perms := map[string]*model.UserPermissions{
			"alice": {ExternalImageView: &storedState},
			"bob":   {ExternalImageView: &stateB},
		}
		m.EXPECT().GetUserPermissionsForAccounts(gomock.Any(), gomock.Any()).Return(perms, nil)

		var log publishLog
		var siteBPublishes atomic.Int32
		publish := func(ctx context.Context, subj string, data []byte) error {
			if strings.Contains(subj, ".site-b.") {
				siteBPublishes.Add(1)
				<-ctx.Done() // site-b stalls until the fanout budget expires
				return ctx.Err()
			}
			if err := ctx.Err(); err != nil { // a real publish fails on an expired budget
				return err
			}
			return log.publish(ctx, subj, data)
		}

		cfg := fanoutTestCfg()
		cfg.FanoutTimeout = 150 * time.Millisecond // keep the stalled lane short in tests
		h := newHandler(m, emptySessionStore(), cfg, nil, publish)

		w := postResync(h, map[string]any{"permission": knownPermission, "accounts": []string{"alice", "bob"}}, t)
		require.Equal(t, http.StatusOK, w.Code)

		body := respBody(t, w)
		assert.Equal(t, []any{"site-b"}, body["syncFailures"],
			"only the stalled peer may be reported — site-c must not inherit an expired ctx from site-b's lane")
		assert.Equal(t, int32(1), siteBPublishes.Load(),
			"once the budget is gone the stalled lane must stop, not fail (and log) every remaining chunk")

		var siteCAccounts []string
		for _, data := range log.data {
			var evt model.InboxEvent
			require.NoError(t, json.Unmarshal(data, &evt))
			var payload model.UserPermissionsUpdated
			require.NoError(t, json.Unmarshal(evt.Payload, &payload))
			siteCAccounts = append(siteCAccounts, payload.Accounts...)
		}
		assert.ElementsMatch(t, []string{"alice", "bob"}, siteCAccounts,
			"site-c must receive BOTH state groups despite site-b stalling")
	})

	t.Run("nil bounds vs bounds set exactly at the Unix epoch are distinct states — must not collide into one group", func(t *testing.T) {
		// Regression pin: the group key used to flatten each bound to UnixMilli, where a
		// nil bound was bit-for-bit identical to a bound set at UnixMilli()==0. Two
		// states sharing Granted+UpdatedAt but differing only in "no window" vs
		// "window pinned to the epoch" must still fan out as two distinct payloads.
		ctrl := gomock.NewController(t)
		m := NewMockAdminStore(ctrl)
		sharedUpdatedAt := time.Date(2025, 6, 1, 8, 30, 0, 0, time.UTC)
		nilBoundsState := model.PermissionState{Granted: true, UpdatedAt: sharedUpdatedAt}
		epochBoundsState := model.PermissionState{
			Granted:       true,
			EffectiveFrom: timePtr(time.UnixMilli(0)),
			ExpiresAt:     timePtr(time.UnixMilli(0)),
			UpdatedAt:     sharedUpdatedAt,
		}
		perms := map[string]*model.UserPermissions{
			"penny": {ExternalImageView: &nilBoundsState},
			"quinn": {ExternalImageView: &epochBoundsState},
		}
		m.EXPECT().GetUserPermissionsForAccounts(gomock.Any(), gomock.Any()).Return(perms, nil)

		var log publishLog

		h := newHandler(m, emptySessionStore(), fanoutTestCfg(), nil, log.publish)
		w := postResync(h, map[string]any{"permission": knownPermission, "accounts": []string{"penny", "quinn"}}, t)
		require.Equal(t, http.StatusOK, w.Code)

		require.Len(t, log.data, 4, "2 distinct groups x 2 remote dests — nil-bounds and epoch-bounds states must not merge into one group")

		hasBoundsByAccount := map[string]bool{}
		for _, data := range log.data {
			var evt model.InboxEvent
			require.NoError(t, json.Unmarshal(data, &evt))
			var payload model.UserPermissionsUpdated
			require.NoError(t, json.Unmarshal(evt.Payload, &payload))
			require.Len(t, payload.Accounts, 1, "each of the two groups here carries exactly one account")
			hasBoundsByAccount[payload.Accounts[0]] = payload.State.EffectiveFrom != nil
		}

		require.Contains(t, hasBoundsByAccount, "penny")
		require.Contains(t, hasBoundsByAccount, "quinn")
		assert.False(t, hasBoundsByAccount["penny"], "penny's state must keep its nil EffectiveFrom")
		assert.True(t, hasBoundsByAccount["quinn"], "quinn's state must keep its explicit (epoch) EffectiveFrom")
	})
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
					{ID: "g1", Permission: model.PermissionExternalImageView, SubjectAccount: "alice", Granted: true, ApplicantAccount: "carol", ApproverAccount: "dave", Reason: "r1", RecordedBy: "p_admin", RecordedAt: time.Now().UTC()},
					{ID: "g2", Permission: model.PermissionKey("other.permission"), SubjectAccount: "bob", Granted: false, ApplicantAccount: "carol", ApproverAccount: "dave", Reason: "r2", RecordedBy: "p_admin", RecordedAt: time.Now().UTC()},
				}
				m.EXPECT().ListPermissionGrants(gomock.Any(), "", model.PermissionKey(""), 1, 20).
					Return(rows, int64(2), nil)
				m.EXPECT().GetUserPermissions(gomock.Any(), gomock.Any()).Times(0)
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
				m.EXPECT().ListPermissionGrants(gomock.Any(), "", model.PermissionExternalImageView, 1, 20).
					Return([]model.PermissionGrant{}, int64(0), nil)
				m.EXPECT().GetUserPermissions(gomock.Any(), gomock.Any()).Times(0)
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
				m.EXPECT().ListPermissionGrants(gomock.Any(), "alice", model.PermissionKey(""), 1, 20).
					Return(nil, int64(0), fmt.Errorf("boom"))
			},
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:  "GetUserPermissions store error → 500",
			query: "?subjectAccount=alice&permission=" + knownPermission,
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().ListPermissionGrants(gomock.Any(), "alice", model.PermissionExternalImageView, 1, 20).
					Return([]model.PermissionGrant{}, int64(0), nil)
				m.EXPECT().GetUserPermissions(gomock.Any(), "alice").
					Return(nil, fmt.Errorf("boom"))
			},
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:  "subjectAccount only, permission omitted → 200, no currentlyGranted key",
			query: "?subjectAccount=alice",
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().ListPermissionGrants(gomock.Any(), "alice", model.PermissionKey(""), 1, 20).
					Return([]model.PermissionGrant{}, int64(0), nil)
				m.EXPECT().GetUserPermissions(gomock.Any(), gomock.Any()).Times(0)
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
			name:  "no permissions snapshot on the account — currentlyGranted present, false",
			query: "?subjectAccount=alice&permission=" + knownPermission,
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().ListPermissionGrants(gomock.Any(), "alice", model.PermissionExternalImageView, 1, 20).
					Return([]model.PermissionGrant{}, int64(0), nil)
				m.EXPECT().GetUserPermissions(gomock.Any(), "alice").
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
					ID: "g1", Permission: model.PermissionExternalImageView,
					SubjectAccount: "alice", Granted: true,
					EffectiveFrom: timePtr(now.Add(-time.Hour)), ExpiresAt: timePtr(now.Add(time.Hour)),
					ApplicantAccount: "carol", ApproverAccount: "dave", Reason: "r", RecordedBy: "p_admin", RecordedAt: now.Add(-time.Hour),
				}
				m.EXPECT().ListPermissionGrants(gomock.Any(), "alice", model.PermissionExternalImageView, 1, 20).
					Return([]model.PermissionGrant{*latest}, int64(1), nil)
				m.EXPECT().GetUserPermissions(gomock.Any(), "alice").
					Return(&model.UserPermissions{
						ExternalImageView: &model.PermissionState{
							Granted:       true,
							EffectiveFrom: timePtr(now.Add(-time.Hour)),
							ExpiresAt:     timePtr(now.Add(time.Hour)),
							UpdatedAt:     now.Add(-time.Hour),
						},
					}, nil)
			},
			wantStatus: http.StatusOK,
			checkBody: func(t *testing.T, body map[string]any) {
				assert.Equal(t, true, body["currentlyGranted"])
			},
		},
		{
			name:  "currently granted state expired → currentlyGranted false",
			query: "?subjectAccount=alice&permission=" + knownPermission,
			setupMock: func(m *MockAdminStore) {
				now := time.Now().UTC()
				m.EXPECT().ListPermissionGrants(gomock.Any(), "alice", model.PermissionExternalImageView, 1, 20).
					Return([]model.PermissionGrant{}, int64(0), nil)
				m.EXPECT().GetUserPermissions(gomock.Any(), "alice").
					Return(&model.UserPermissions{
						ExternalImageView: &model.PermissionState{
							Granted:       true,
							EffectiveFrom: timePtr(now.Add(-2 * time.Hour)),
							ExpiresAt:     timePtr(now.Add(-time.Hour)),
							UpdatedAt:     now.Add(-2 * time.Hour),
						},
					}, nil)
			},
			wantStatus: http.StatusOK,
			checkBody: func(t *testing.T, body map[string]any) {
				val, ok := body["currentlyGranted"]
				require.True(t, ok)
				assert.Equal(t, false, val)
			},
		},
		{
			name:  "ledger with a revoke row and a grant row: shapes differ",
			query: "?subjectAccount=alice",
			setupMock: func(m *MockAdminStore) {
				grantRow := model.PermissionGrant{
					ID: "g-grant", Permission: model.PermissionExternalImageView,
					SubjectAccount: "alice", Granted: true,
					EffectiveFrom:    timePtr(time.Date(2026, 9, 1, 0, 0, 0, 0, tzTaipei)),
					ExpiresAt:        timePtr(time.Date(2027, 1, 1, 0, 0, 0, 0, tzTaipei)),
					ApplicantAccount: "carol", ApproverAccount: "dave", Reason: "r1",
					RecordedBy: "p_admin", RecordedAt: time.Date(2026, 9, 1, 1, 0, 0, 0, time.UTC),
				}
				revokeRow := model.PermissionGrant{
					ID: "g-revoke", Permission: model.PermissionExternalImageView,
					SubjectAccount: "alice", Granted: false,
					ApplicantAccount: "carol", ApproverAccount: "dave", Reason: "r2",
					RecordedBy: "p_admin", RecordedAt: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC),
				}
				m.EXPECT().ListPermissionGrants(gomock.Any(), "alice", model.PermissionKey(""), 1, 20).
					Return([]model.PermissionGrant{revokeRow, grantRow}, int64(2), nil)
				m.EXPECT().GetUserPermissions(gomock.Any(), gomock.Any()).Times(0)
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
				assert.False(t, hasFrom, "revoke row must not carry an effectiveFrom key")
				assert.False(t, hasUntil, "revoke row must not carry an expiresAt key")

				grant, ok := entries[1].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "g-grant", grant["id"])
				assert.Equal(t, "2026-09-01", grant["effectiveFrom"])
				assert.Equal(t, "2026-12-31", grant["expiresAt"])
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			m := NewMockAdminStore(ctrl)
			tc.setupMock(m)

			h := newHandler(m, emptySessionStore(), testCfg(), nil, nil)
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
