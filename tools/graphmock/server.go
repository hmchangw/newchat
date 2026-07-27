package main

import (
	"fmt"
	"net/http"
	"strconv"
	"sync"

	"github.com/gin-gonic/gin"
)

// fixtures is the mock tenant dataset. Groups carry raw member objects (any
// element shape, incl. non-user members); users/chats are the surfaces the
// user-sync, chat-sync, and member-sync stages read.
type fixtures struct {
	Groups []group          `json:"groups"`
	Users  []map[string]any `json:"users"`
	Chats  []chat           `json:"chats"`
}

type group struct {
	ID          string           `json:"id"`
	DisplayName string           `json:"displayName"`
	Description string           `json:"description"`
	Members     []map[string]any `json:"members"`
}

// chat mirrors the Graph chat shape the sync reads (GET /users/{id}/chats with
// $expand=members). Membership drives both endpoints: /users/{id}/chats returns
// chats whose members include the user, /chats/{id}/members returns Members.
type chat struct {
	ID                  string       `json:"id"`
	ChatType            string       `json:"chatType"`
	Topic               string       `json:"topic"`
	CreatedDateTime     string       `json:"createdDateTime"`
	LastUpdatedDateTime string       `json:"lastUpdatedDateTime"`
	Members             []chatMember `json:"members"`
}

type chatMember struct {
	UserID                      string `json:"userId"`
	VisibleHistoryStartDateTime string `json:"visibleHistoryStartDateTime,omitempty"`
}

// server holds the swappable dataset behind a mutex (PUT /__fixtures replaces
// it at runtime).
type server struct {
	mu   sync.RWMutex
	data fixtures
}

func (s *server) group(id string) (group, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, g := range s.data.Groups {
		if g.ID == id {
			return g, true
		}
	}
	return group{}, false
}

func (s *server) chat(id string) (chat, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, c := range s.data.Chats {
		if c.ID == id {
			return c, true
		}
	}
	return chat{}, false
}

func newRouter(s *server) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	r.GET("/healthz", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"status": "ok"}) })
	// client-credentials grant — any tenant/credentials accepted
	r.POST("/:tenant/oauth2/v2.0/token", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"access_token": "graphmock-token", "token_type": "Bearer", "expires_in": 3600})
	})
	v1 := r.Group("/v1.0")
	v1.GET("/groups/:id", s.getGroup)
	v1.GET("/groups/:id/members", s.listMembers)
	v1.GET("/users", s.listUsers)
	v1.GET("/users/:id/chats", s.listUserChats)
	v1.GET("/chats/:id/members", s.listChatMembers)
	r.GET("/__fixtures", func(c *gin.Context) {
		s.mu.RLock()
		defer s.mu.RUnlock()
		c.JSON(http.StatusOK, s.data)
	})
	r.PUT("/__fixtures", func(c *gin.Context) {
		var f fixtures
		if err := c.ShouldBindJSON(&f); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		s.mu.Lock()
		s.data = f
		s.mu.Unlock()
		c.JSON(http.StatusOK, gin.H{"groups": len(f.Groups), "users": len(f.Users), "chats": len(f.Chats)})
	})
	return r
}

func (s *server) getGroup(c *gin.Context) {
	g, ok := s.group(c.Param("id"))
	if !ok {
		graphNotFound(c)
		return
	}
	c.JSON(http.StatusOK, gin.H{"id": g.ID, "displayName": g.DisplayName, "description": g.Description})
}

func (s *server) listMembers(c *gin.Context) {
	g, ok := s.group(c.Param("id"))
	if !ok {
		graphNotFound(c)
		return
	}
	writePage(c, g.Members, fmt.Sprintf("/v1.0/groups/%s/members", g.ID))
}

func (s *server) listUsers(c *gin.Context) {
	s.mu.RLock()
	users := s.data.Users
	s.mu.RUnlock()
	writePage(c, users, "/v1.0/users")
}

// listUserChats returns the chats the user is a member of — Graph filters
// server-side; the mock derives membership from each chat's Members.
func (s *server) listUserChats(c *gin.Context) {
	userID := c.Param("id")
	s.mu.RLock()
	var out []chat
	for _, ch := range s.data.Chats {
		for _, m := range ch.Members {
			if m.UserID == userID {
				out = append(out, ch)
				break
			}
		}
	}
	s.mu.RUnlock()
	writePage(c, out, fmt.Sprintf("/v1.0/users/%s/chats", userID))
}

func (s *server) listChatMembers(c *gin.Context) {
	ch, ok := s.chat(c.Param("id"))
	if !ok {
		graphNotFound(c)
		return
	}
	writePage(c, ch.Members, fmt.Sprintf("/v1.0/chats/%s/members", ch.ID))
}

// writePage serves one $top-sized page of items and emits a self-pointing
// @odata.nextLink (?$skip=N) so real Graph clients exercise their pager. A bad
// $top writes a 400 and returns without a body.
func writePage[T any](c *gin.Context, items []T, path string) {
	top := len(items)
	if v := c.Query("$top"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 999 {
			c.JSON(http.StatusBadRequest, gin.H{"error": gin.H{"code": "Request_BadRequest", "message": "invalid $top"}})
			return
		}
		top = n
	}
	skip, _ := strconv.Atoi(c.Query("$skip"))
	if skip < 0 || skip > len(items) {
		skip = len(items)
	}
	end := min(skip+top, len(items))
	page := gin.H{"value": items[skip:end]}
	if end < len(items) {
		page["@odata.nextLink"] = fmt.Sprintf("http://%s%s?$skip=%d&$top=%d", c.Request.Host, path, end, top)
	}
	c.JSON(http.StatusOK, page)
}

func graphNotFound(c *gin.Context) {
	c.JSON(http.StatusNotFound, gin.H{"error": gin.H{
		"code": "Request_ResourceNotFound", "message": "resource not found",
	}})
}
