package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
	"github.com/hmchangw/chat/pkg/model"
)

type (
	botSendMessageRequest  = model.BotSendMessageRequest
	botCreateRoomRequest   = model.BotCreateRoomRequest
	botMembersBatchRequest = model.BotMembersBatchRequest
)

const (
	botBatchMaxUserIDs = 100
	botBatchMaxOrgIDs  = 5
)

// botSendRoomMessage routes to the room's home site via subscription lookup (design 2026-07-22-bot-cross-site-routing §3).
func (h *handler) botSendRoomMessage(c *gin.Context) {
	var req botSendMessageRequest
	if !bindStrict(c, &req) {
		return
	}
	if !validateSendContent(c, req.Content) {
		return
	}
	sess := botPrincipalFrom(c)
	roomID := c.Param("roomID")
	sub, err := h.subs.FindForBot(c.Request.Context(), sess.UserID, roomID)
	if err != nil {
		errhttp.Write(c.Request.Context(), c, notMemberOrInternal(err))
		return
	}
	body, err := json.Marshal(req)
	if err != nil {
		errhttp.Write(c.Request.Context(), c, errcode.Internal("re-marshal bot request", errcode.WithCause(err)))
		return
	}
	msg, err := h.forwarder.sendRoom(c.Request.Context(), sess, sub.SiteID, roomID, body)
	if err != nil {
		errhttp.Write(c.Request.Context(), c, err)
		return
	}
	c.JSON(http.StatusOK, msg)
}

// botSendDMMessage looks up the DM subscription and routes to that site. On miss (first-time
// DM) it calls dm.ensure at BP's own site to create the room, then forwards send-DM locally.
func (h *handler) botSendDMMessage(c *gin.Context) {
	var req botSendMessageRequest
	if !bindStrict(c, &req) {
		return
	}
	if !validateSendContent(c, req.Content) {
		return
	}
	sess := botPrincipalFrom(c)
	targetUserID := c.Param("userID")

	targetSiteID := h.cfg.SiteID
	sub, err := h.subs.FindDMForBot(c.Request.Context(), sess.UserID, targetUserID)
	switch {
	case err == nil:
		targetSiteID = sub.SiteID
	case errors.Is(err, model.ErrSubscriptionNotFound):
		if _, err := h.dmEnsurer.Ensure(c.Request.Context(), sess, targetUserID); err != nil {
			errhttp.Write(c.Request.Context(), c, err)
			return
		}
	default:
		errhttp.Write(c.Request.Context(), c, fmt.Errorf("find dm subscription: %w", err))
		return
	}

	body, err := json.Marshal(req)
	if err != nil {
		errhttp.Write(c.Request.Context(), c, errcode.Internal("re-marshal bot request", errcode.WithCause(err)))
		return
	}
	msg, err := h.forwarder.sendDM(c.Request.Context(), sess, targetSiteID, targetUserID, body)
	if err != nil {
		errhttp.Write(c.Request.Context(), c, err)
		return
	}
	c.JSON(http.StatusOK, msg)
}

func (h *handler) botCreateRoom(c *gin.Context) {
	var req botCreateRoomRequest
	if !bindStrict(c, &req) {
		return
	}
	if strings.TrimSpace(req.Name) == "" {
		writeBadRequest(c, "name is required", errcode.BotContentInvalid)
		return
	}
	if !validateBatchSizes(c, len(req.Members), len(req.Orgs)) {
		return
	}
	body, err := json.Marshal(req)
	if err != nil {
		errhttp.Write(c.Request.Context(), c, errcode.Internal("re-marshal bot request", errcode.WithCause(err)))
		return
	}
	replyData, err := h.forwarder.createRoom(c.Request.Context(), botPrincipalFrom(c), h.cfg.SiteID, body)
	if err != nil {
		errhttp.Write(c.Request.Context(), c, err)
		return
	}
	c.Data(http.StatusOK, "application/json", replyData)
}

func (h *handler) botAddMembers(c *gin.Context) {
	h.handleMembersBatch(c, botAddOp)
}

func (h *handler) botRemoveMembers(c *gin.Context) {
	h.handleMembersBatch(c, botRemoveOp)
}

type memberBatchOp int

const (
	botAddOp memberBatchOp = iota
	botRemoveOp
)

// handleMembersBatch decodes, subscription-routes to the room's home site, and forwards.
func (h *handler) handleMembersBatch(c *gin.Context, op memberBatchOp) {
	var req botMembersBatchRequest
	if !bindStrict(c, &req) {
		return
	}
	if len(req.UserIDs) == 0 && len(req.OrgIDs) == 0 {
		writeBadRequest(c, "at least one of userIds or orgIds must be non-empty", errcode.BotContentInvalid)
		return
	}
	if !validateBatchSizes(c, len(req.UserIDs), len(req.OrgIDs)) {
		return
	}
	sess := botPrincipalFrom(c)
	roomID := c.Param("roomID")
	sub, err := h.subs.FindForBot(c.Request.Context(), sess.UserID, roomID)
	if err != nil {
		errhttp.Write(c.Request.Context(), c, notMemberOrInternal(err))
		return
	}
	body, err := json.Marshal(req)
	if err != nil {
		errhttp.Write(c.Request.Context(), c, errcode.Internal("re-marshal bot request", errcode.WithCause(err)))
		return
	}
	var replyData []byte
	switch op {
	case botAddOp:
		replyData, err = h.forwarder.addMembers(c.Request.Context(), sess, sub.SiteID, roomID, body)
	case botRemoveOp:
		replyData, err = h.forwarder.removeMembers(c.Request.Context(), sess, sub.SiteID, roomID, body)
	}
	if err != nil {
		errhttp.Write(c.Request.Context(), c, err)
		return
	}
	c.Data(http.StatusOK, "application/json", replyData)
}

// notMemberOrInternal maps a subscription lookup error to the wire envelope: a miss surfaces as 403 not_a_room_member; anything else is infra.
func notMemberOrInternal(err error) error {
	if errors.Is(err, model.ErrSubscriptionNotFound) {
		return errcode.Forbidden("bot is not a room member",
			errcode.WithReason(errcode.BotNotARoomMember))
	}
	return fmt.Errorf("find subscription: %w", err)
}

// bindStrict decodes into out with DisallowUnknownFields so extraneous fields (e.g. attachments) 400.
// Body is capped so oversized requests fail during read, not after a full decode.
const botRequestBodyMaxBytes = model.BotContentMaxBytes + 76*1024

func bindStrict(c *gin.Context, out any) bool {
	body, err := io.ReadAll(http.MaxBytesReader(c.Writer, c.Request.Body, botRequestBodyMaxBytes))
	if err != nil {
		writeBadRequest(c, "request body too large", errcode.BotContentInvalid)
		return false
	}
	if len(body) == 0 {
		writeBadRequest(c, "request body is required", errcode.BotContentInvalid)
		return false
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		var unknownErr *json.SyntaxError
		if errors.As(err, &unknownErr) {
			writeBadRequest(c, "malformed json body", errcode.BotContentInvalid)
			return false
		}
		msg := err.Error()
		if strings.Contains(msg, "unknown field") {
			writeBadRequest(c, msg, errcode.BotUnknownField)
			return false
		}
		writeBadRequest(c, "invalid request body", errcode.BotContentInvalid)
		return false
	}
	// Reject concatenated / trailing JSON — Decode stops after the first value.
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		writeBadRequest(c, "invalid request body", errcode.BotContentInvalid)
		return false
	}
	return true
}

// validateSendContent enforces 1 <= len(content) <= BotContentMaxBytes (bytes, not runes).
func validateSendContent(c *gin.Context, content string) bool {
	if len(content) == 0 {
		writeBadRequest(c, "content is required", errcode.BotContentInvalid)
		return false
	}
	if len(content) > model.BotContentMaxBytes {
		writeBadRequest(c,
			fmt.Sprintf("content exceeds %d-byte limit (got %d)", model.BotContentMaxBytes, len(content)),
			errcode.BotContentInvalid)
		return false
	}
	return true
}

// validateBatchSizes enforces the outer 100/5 userIds/orgIds caps.
func validateBatchSizes(c *gin.Context, userCount, orgCount int) bool {
	if userCount > botBatchMaxUserIDs {
		writeBadRequest(c,
			fmt.Sprintf("userIds exceeds batch limit (max %d, got %d)", botBatchMaxUserIDs, userCount),
			errcode.BotBatchTooLarge)
		return false
	}
	if orgCount > botBatchMaxOrgIDs {
		writeBadRequest(c,
			fmt.Sprintf("orgIds exceeds batch limit (max %d, got %d)", botBatchMaxOrgIDs, orgCount),
			errcode.BotBatchTooLarge)
		return false
	}
	return true
}

func writeBadRequest(c *gin.Context, msg string, reason errcode.Reason) {
	errhttp.Write(c.Request.Context(), c, errcode.BadRequest(msg, errcode.WithReason(reason)))
}
