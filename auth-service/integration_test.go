//go:build integration

package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/testutil"
)

// fakeValidator is defined in handler_test.go (same package). The integration
// test reuses it rather than declaring its own to avoid name collisions.

func TestAuthHandler_Integration(t *testing.T) {
	kp, err := nkeys.CreateAccount()
	require.NoError(t, err)
	accPub, err := kp.PublicKey()
	require.NoError(t, err)

	userKP, err := nkeys.CreateUser()
	require.NoError(t, err)
	userPub, err := userKP.PublicKey()
	require.NoError(t, err)

	validator := &fakeValidator{
		account:     "testuser",
		subject:     "uuid-testuser",
		email:       "testuser@example.com",
		description: "E001, Test User, 測試用戶",
		deptName:    "QA",
		deptId:      "ABC",
	}
	handler := NewAuthHandler(validator, kp, accPub, 2*time.Hour, false)

	gin.SetMode(gin.TestMode)
	r := gin.New()
	registerRoutes(r, handler)

	body := fmt.Sprintf(`{"ssoToken":"valid-token","natsPublicKey":"%s"}`, userPub)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)

	var resp authResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))

	// Decode and verify the JWT.
	claims, err := jwt.DecodeUserClaims(resp.NATSJWT)
	require.NoError(t, err)

	// Perms and limits live on the scoped signing key template; the JWT
	// only stamps the account tag for {{tag(account)}} substitution and
	// issuer_account so the NATS resolver can attribute the SK.
	assert.Contains(t, claims.Tags, "account:testuser")
	assert.Equal(t, accPub, claims.IssuerAccount)
	assert.Empty(t, claims.Pub.Allow)
	assert.Empty(t, claims.Sub.Allow)
	assert.Equal(t, jwt.UserPermissionLimits{}, claims.UserPermissionLimits)
}

func TestMain(m *testing.M) { testutil.RunTests(m) }
