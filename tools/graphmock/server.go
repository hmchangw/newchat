package main

import (
	"fmt"
	"net/http"
	"strconv"
	"sync"

	"github.com/gin-gonic/gin"
)

// fixtures is the mock tenant dataset: groups with raw member objects
// (kept as maps so fixtures can carry any element shape, incl. non-user
// members).
type fixtures struct {
	Groups []group `json:"groups"`
}

type group struct {
	ID          string           `json:"id"`
	DisplayName string           `json:"displayName"`
	Description string           `json:"description"`
	Members     []map[string]any `json:"members"`
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
		c.JSON(http.StatusOK, gin.H{"groups": len(f.Groups)})
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

// listMembers serves one $top-sized page and emits a self-pointing
// @odata.nextLink (?$skip=N) so real Graph clients exercise their pager.
func (s *server) listMembers(c *gin.Context) {
	g, ok := s.group(c.Param("id"))
	if !ok {
		graphNotFound(c)
		return
	}
	top := len(g.Members)
	if v := c.Query("$top"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 999 {
			c.JSON(http.StatusBadRequest, gin.H{"error": gin.H{"code": "Request_BadRequest", "message": "invalid $top"}})
			return
		}
		top = n
	}
	skip, _ := strconv.Atoi(c.Query("$skip"))
	if skip < 0 || skip > len(g.Members) {
		skip = len(g.Members)
	}
	end := min(skip+top, len(g.Members))
	page := gin.H{"value": g.Members[skip:end]}
	if end < len(g.Members) {
		page["@odata.nextLink"] = fmt.Sprintf("http://%s/v1.0/groups/%s/members?$skip=%d&$top=%d",
			c.Request.Host, g.ID, end, top)
	}
	c.JSON(http.StatusOK, page)
}

func graphNotFound(c *gin.Context) {
	c.JSON(http.StatusNotFound, gin.H{"error": gin.H{
		"code": "Request_ResourceNotFound", "message": "resource not found",
	}})
}
