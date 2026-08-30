package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

// hubFactory returns a factory that hands out the supplied mock hubs in order,
// along with a pointer to the call counter.
func hubFactory(hubs ...Hub) (func() Hub, *int) {
	calls := 0
	i := 0
	return func() Hub {
		calls++
		h := hubs[i]
		i++
		return h
	}, &calls
}

func sessionCookieValue(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	for _, c := range rec.Result().Cookies() {
		if c.Name == sessionCookieName {
			return c.Value
		}
	}
	t.Fatalf("no %q cookie set on response", sessionCookieName)
	return ""
}

func TestSessionManager_Resolve_SetsCookieWhenAbsent(t *testing.T) {
	ctrl := gomock.NewController(t)
	factory, calls := hubFactory(NewMockHub(ctrl))
	sm := newSessionManager(factory, time.Hour)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)

	sess := sm.resolve(rec, req)

	require.NotNil(t, sess)
	assert.Equal(t, 1, *calls)

	var cookie *http.Cookie
	for _, c := range rec.Result().Cookies() {
		if c.Name == sessionCookieName {
			cookie = c
		}
	}
	require.NotNil(t, cookie, "expected session cookie to be set")
	assert.Equal(t, sess.id, cookie.Value)
	assert.True(t, cookie.HttpOnly)
	assert.Equal(t, "/", cookie.Path)
	assert.Equal(t, http.SameSiteStrictMode, cookie.SameSite)
}

func TestSessionManager_Resolve_ReusesSessionForSameCookie(t *testing.T) {
	ctrl := gomock.NewController(t)
	factory, calls := hubFactory(NewMockHub(ctrl))
	sm := newSessionManager(factory, time.Hour)

	rec1 := httptest.NewRecorder()
	first := sm.resolve(rec1, httptest.NewRequest(http.MethodGet, "/", nil))
	id := sessionCookieValue(t, rec1)

	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodGet, "/", nil)
	// local debug tool served over plain HTTP on localhost
	// nosemgrep: cookie-missing-httponly, cookie-missing-secure
	req2.AddCookie(&http.Cookie{Name: sessionCookieName, Value: id})
	second := sm.resolve(rec2, req2)

	assert.Same(t, first, second)
	assert.Equal(t, 1, *calls, "factory should only run once for the same cookie")
}

func TestSessionManager_Resolve_DistinctCookiesDistinctHubs(t *testing.T) {
	ctrl := gomock.NewController(t)
	hubA := NewMockHub(ctrl)
	hubB := NewMockHub(ctrl)
	factory, calls := hubFactory(hubA, hubB)
	sm := newSessionManager(factory, time.Hour)

	a := sm.resolve(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	b := sm.resolve(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))

	assert.NotEqual(t, a.id, b.id)
	assert.NotSame(t, a, b)
	assert.Same(t, hubA, a.hub)
	assert.Same(t, hubB, b.hub)
	assert.Equal(t, 2, *calls)
}

func TestSessionManager_Sweep_RemovesIdle(t *testing.T) {
	ctrl := gomock.NewController(t)
	hub := NewMockHub(ctrl)
	hub.EXPECT().Disconnect()

	factory, _ := hubFactory(hub)
	sm := newSessionManager(factory, 30*time.Minute)

	base := time.Date(2026, 6, 15, 8, 0, 0, 0, time.UTC)
	sm.now = func() time.Time { return base }
	sm.resolve(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))

	sm.now = func() time.Time { return base.Add(31 * time.Minute) }
	sm.sweep()

	assert.Empty(t, sm.sessions)
}

func TestSessionManager_Sweep_KeepsRecent(t *testing.T) {
	ctrl := gomock.NewController(t)
	// No Disconnect expectation: a recent session must not be torn down.
	factory, _ := hubFactory(NewMockHub(ctrl))
	sm := newSessionManager(factory, 30*time.Minute)

	base := time.Date(2026, 6, 15, 8, 0, 0, 0, time.UTC)
	sm.now = func() time.Time { return base }
	sm.resolve(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))

	sm.now = func() time.Time { return base.Add(5 * time.Minute) }
	sm.sweep()

	assert.Len(t, sm.sessions, 1)
}

func TestSessionManager_Touch_UpdatesLastSeen(t *testing.T) {
	ctrl := gomock.NewController(t)
	factory, _ := hubFactory(NewMockHub(ctrl))
	sm := newSessionManager(factory, time.Hour)

	base := time.Date(2026, 6, 15, 8, 0, 0, 0, time.UTC)
	sm.now = func() time.Time { return base }
	sess := sm.resolve(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))

	later := base.Add(10 * time.Minute)
	sm.now = func() time.Time { return later }
	sm.touch(sess)

	assert.Equal(t, time.Duration(0), sess.idle(later))
}

func TestSessionManager_Start_SweepsIdle(t *testing.T) {
	ctrl := gomock.NewController(t)
	hub := NewMockHub(ctrl)
	swept := make(chan struct{})
	hub.EXPECT().Disconnect().Do(func() { close(swept) })

	factory, _ := hubFactory(hub)
	sm := newSessionManager(factory, 20*time.Millisecond)
	sm.resolve(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))

	sm.start()
	defer sm.shutdown()

	select {
	case <-swept:
	case <-time.After(2 * time.Second):
		t.Fatal("idle session was not swept by the janitor")
	}
}

func TestSessionManager_Shutdown_DisconnectsAll(t *testing.T) {
	ctrl := gomock.NewController(t)
	hubA := NewMockHub(ctrl)
	hubB := NewMockHub(ctrl)
	hubA.EXPECT().Disconnect()
	hubB.EXPECT().Disconnect()

	factory, _ := hubFactory(hubA, hubB)
	sm := newSessionManager(factory, time.Hour)

	sm.resolve(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	sm.resolve(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))

	sm.shutdown()

	assert.Empty(t, sm.sessions)
}
