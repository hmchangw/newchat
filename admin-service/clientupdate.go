package main

import (
	"context"
	"mime/multipart"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
)

// auditActionClientUpdateUpload names the ledger entry for a published update.
// Because admin-service is the only route to client-update-service, this entry
// is a complete record: no artifact reaches storage without one.
const auditActionClientUpdateUpload = "client_update.upload"

// clientUpdateUploader is the forwarding surface this handler needs, declared
// here — at the consumer — so tests can substitute a fake.
type clientUpdateUploader interface {
	Forward(ctx context.Context, src *multipart.Reader) (uploadedNames, error)
}

// uploadClientVersion streams an update-artifact pair through to
// client-update-service under this service's own service-account token.
//
// The body is read with MultipartReader rather than c.FormFile: Gin's form
// parsing buffers to memory and spills to local disk, which a large executable
// must not do in a pod. Validation of the pair itself happens downstream —
// a missing part is only knowable at EOF, by which point the upload is spent.
func (h *Handler) uploadClientVersion(c *gin.Context) {
	ctx := c.Request.Context()

	if h.clientUpdate == nil {
		errhttp.Write(ctx, c, errcode.Unavailable("client update publishing is not configured on this site",
			errcode.WithReason(errcode.AdminUpstreamUnavailable)))
		return
	}

	src, err := c.Request.MultipartReader()
	if err != nil {
		errhttp.Write(ctx, c, errcode.BadRequest("request body must be multipart/form-data",
			errcode.WithCause(err)))
		return
	}

	names, err := h.clientUpdate.Forward(ctx, src)
	if err != nil {
		errhttp.Write(ctx, c, err)
		return
	}

	h.audit(ctx, c, auditActionClientUpdateUpload, "", "", map[string]string{
		"configFile":  names.ConfigFile,
		"executeFile": names.ExecuteFile,
	})
	c.JSON(http.StatusOK, gin.H{"result": "success"})
}
