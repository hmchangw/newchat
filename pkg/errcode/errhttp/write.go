// Package errhttp adapts errcode.Error to Gin HTTP responses.
package errhttp

import (
	"context"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/errcode"
)

// ErrCodeKey is the gin.Context key under which Write records the classified
// errcode Code, so metrics middleware can label the response without
// re-classifying (which would double-log).
const ErrCodeKey = "errcode"

// Write classifies err (logging once) and writes the envelope with its HTTP
// status. It also records the classified Code on the gin context under
// ErrCodeKey for downstream metrics middleware.
func Write(ctx context.Context, c *gin.Context, err error) {
	e := errcode.Classify(ctx, err)
	c.Set(ErrCodeKey, string(e.Code))
	c.JSON(e.HTTPStatus(), e)
}
