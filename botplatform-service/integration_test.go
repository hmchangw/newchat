//go:build integration

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"golang.org/x/crypto/bcrypt"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) {
	testutil.RunTestsWithPrewarm(m, testutil.EnsureMongo)
}

// seedUser inserts a user with bcrypt(sha256_hex(plaintext)) using the legacy
// recipe, returning the user doc for assertions.
func seedUser(t *testing.T, db *mongo.Database, id, account, siteID, plaintext string, roles []model.UserRole) model.User {
	t.Helper()
	sum := sha256.Sum256([]byte(plaintext))
	hash, err := bcrypt.GenerateFromPassword([]byte(hex.EncodeToString(sum[:])), bcrypt.MinCost)
	require.NoError(t, err)
	u := model.User{
		ID:       id,
		Account:  account,
		SiteID:   siteID,
		Roles:    roles,
		Services: model.Services{Password: model.PasswordCredentials{Bcrypt: string(hash)}},
	}
	_, err = db.Collection("users").InsertOne(context.Background(), u)
	require.NoError(t, err)
	return u
}

func newIntegrationRouter(t *testing.T, db *mongo.Database, cfg *config) *gin.Engine {
	t.Helper()
	gin.SetMode(gin.TestMode)
	st, err := newMongoStore(context.Background(), db)
	require.NoError(t, err)
	h := newHandler(st, cfg)
	r := gin.New()
	registerRoutes(r, h)
	return r
}

func httpPost(t *testing.T, r http.Handler, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

// End-to-end login → validate with real Mongo: seed a user, log in, capture
// the returned authToken, validate it, assert the principal.
func TestIntegration_LoginThenValidate(t *testing.T) {
	db := testutil.MongoDB(t, "bp_login")
	cfg := &config{SiteID: "site-a", SessionsMaxPerAccount: 100, BcryptCost: bcrypt.MinCost}
	u := seedUser(t, db, "user000000000000a", "x.shortcode.bot", "site-a", "secret",
		[]model.UserRole{model.UserRoleBot})
	r := newIntegrationRouter(t, db, cfg)

	// Login.
	w := httpPost(t, r, "/api/v1/login", map[string]string{"username": u.Account, "password": "secret"})
	require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())
	var resp loginResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	authToken := resp.Data.AuthToken
	assert.Len(t, authToken, 43, "token must be 43-char base64url")
	assert.Equal(t, u.ID, resp.Data.UserID)
	assert.Equal(t, []string{"bot"}, resp.Data.Me.Roles)

	// Validate.
	w = httpPost(t, r, "/api/v1/auth/validate", map[string]string{"authToken": authToken})
	require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())
	var vr validateResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &vr))
	assert.True(t, vr.Valid)
	assert.Equal(t, u.ID, vr.Principal.UserID)
	assert.Equal(t, u.Account, vr.Principal.Account)
	assert.Equal(t, u.SiteID, vr.Principal.SiteID)
	assert.Equal(t, []string{"bot"}, vr.Principal.Roles)
}

// The sessions collection must carry the {_id:1} primary and the compound
// {userId:1, issuedAt:1} index used by FIFO eviction. We use
// ListSpecifications (typed) instead of List + bson.M decode because the v2
// driver's inner-document type for the `key` field varies and the typed
// surface gives us the auto-generated index name verbatim.
func TestIntegration_SessionsIndexes(t *testing.T) {
	db := testutil.MongoDB(t, "bp_idx")
	cfg := &config{SiteID: "site-a", SessionsMaxPerAccount: 100, BcryptCost: bcrypt.MinCost}
	_ = newIntegrationRouter(t, db, cfg) // triggers ensureIndexes via newMongoStore

	specs, err := db.Collection("sessions").Indexes().ListSpecifications(context.Background())
	require.NoError(t, err)

	names := make([]string, 0, len(specs))
	for _, s := range specs {
		names = append(names, s.Name)
	}
	// Mongo auto-names compound indexes as <field1>_<dir1>_<field2>_<dir2>.
	assert.Contains(t, names, "userId_1_issuedAt_1",
		"expected compound {userId:1, issuedAt:1} index; got specs: %+v", names)
}

// Real FIFO eviction: cap=3, issue 5 logins for the same user, assert
// exactly 3 sessions remain (oldest 2 gone), and the 2 earliest tokens fail
// validate.
func TestIntegration_FIFOEviction(t *testing.T) {
	db := testutil.MongoDB(t, "bp_evict")
	cfg := &config{SiteID: "site-a", SessionsMaxPerAccount: 3, BcryptCost: bcrypt.MinCost}
	u := seedUser(t, db, "userevict00000001", "evictor.shortcode.bot", "site-a", "p",
		[]model.UserRole{model.UserRoleBot})
	r := newIntegrationRouter(t, db, cfg)

	tokens := make([]string, 0, 5)
	for range 5 {
		w := httpPost(t, r, "/api/v1/login", map[string]string{"username": u.Account, "password": "p"})
		require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())
		var resp loginResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		tokens = append(tokens, resp.Data.AuthToken)
	}

	count, err := db.Collection("sessions").CountDocuments(context.Background(), bson.M{"userId": u.ID})
	require.NoError(t, err)
	assert.Equal(t, int64(3), count, "exactly cap (3) sessions should remain")

	// The two oldest tokens (tokens[0], tokens[1]) must now 401 on validate.
	for i := range 2 {
		w := httpPost(t, r, "/api/v1/auth/validate", map[string]string{"authToken": tokens[i]})
		assert.Equalf(t, http.StatusUnauthorized, w.Code,
			"evicted token tokens[%d] should not validate; body=%s", i, w.Body.String())
	}
	// The three newest tokens (tokens[2..4]) must validate.
	for i := 2; i < 5; i++ {
		w := httpPost(t, r, "/api/v1/auth/validate", map[string]string{"authToken": tokens[i]})
		assert.Equalf(t, http.StatusOK, w.Code,
			"surviving token tokens[%d] should validate; body=%s", i, w.Body.String())
	}
}

// Cross-account safety: evicting user A's sessions must NEVER touch user B's
// sessions. cap=1 on a per-user basis.
func TestIntegration_CrossAccountSafety(t *testing.T) {
	db := testutil.MongoDB(t, "bp_cross")
	cfg := &config{SiteID: "site-a", SessionsMaxPerAccount: 1, BcryptCost: bcrypt.MinCost}
	uA := seedUser(t, db, "useraaaaaaaaaaaaa", "a.s.bot", "site-a", "p", []model.UserRole{model.UserRoleBot})
	uB := seedUser(t, db, "userbbbbbbbbbbbbb", "b.s.bot", "site-a", "p", []model.UserRole{model.UserRoleBot})
	r := newIntegrationRouter(t, db, cfg)

	// Both users log in to cap.
	_ = httpPost(t, r, "/api/v1/login", map[string]string{"username": uA.Account, "password": "p"})
	_ = httpPost(t, r, "/api/v1/login", map[string]string{"username": uB.Account, "password": "p"})

	// A logs in again — should evict A's older session but leave B's alone.
	_ = httpPost(t, r, "/api/v1/login", map[string]string{"username": uA.Account, "password": "p"})

	for _, u := range []model.User{uA, uB} {
		c, err := db.Collection("sessions").CountDocuments(context.Background(), bson.M{"userId": u.ID})
		require.NoError(t, err)
		assert.Equalf(t, int64(1), c, "user %s should have exactly 1 session (cap), got %d", u.Account, c)
	}
}

// Cross-site login: a user whose siteId differs from cfg.SiteID may still log
// in — any user may authenticate against any cluster, and a session is written.
func TestIntegration_CrossSiteLoginAllowed(t *testing.T) {
	db := testutil.MongoDB(t, "bp_crosssite")
	cfg := &config{SiteID: "site-a", SessionsMaxPerAccount: 100, BcryptCost: bcrypt.MinCost}
	u := seedUser(t, db, "usercrosssite0000", "x.s.bot", "site-OTHER", "p",
		[]model.UserRole{model.UserRoleBot})
	r := newIntegrationRouter(t, db, cfg)

	w := httpPost(t, r, "/api/v1/login", map[string]string{"username": u.Account, "password": "p"})
	assert.Equal(t, http.StatusOK, w.Code)

	c, err := db.Collection("sessions").CountDocuments(context.Background(), bson.M{"userId": u.ID})
	require.NoError(t, err)
	assert.Equal(t, int64(1), c, "a session should be written even for a cross-site user")
}
