//go:build integration

package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	o11ynats "github.com/flywindy/o11y/nats"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsmetrics"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }

// Registers the real handler on a natsrouter against a live NATS server and
// exercises the synchronous request/reply path end to end: a success reply and
// an error envelope both flow back on the caller's _INBOX subject.
func TestTranslate_EndToEnd(t *testing.T) {
	nc, err := o11ynats.Connect(context.Background(), testutil.NATS(t),
		noop.NewTracerProvider(), propagation.TraceContext{})
	require.NoError(t, err)
	t.Cleanup(nc.Close)

	const siteID = "site-a"
	router := natsrouter.Default(nc, "translation-service")
	natsrouter.Register(router, subject.TranslateRequestPattern(siteID), natsmetrics.MethodTranslateText, NewHandler(mockTranslator{}).Translate)
	t.Cleanup(func() { require.NoError(t, router.Shutdown(context.Background())) })

	reqSubject := subject.TranslateRequest("alice", siteID)

	t.Run("success reply", func(t *testing.T) {
		// BCP-47 targetLang from settings; the mock backend receives the mapped code (zhTW).
		body, err := json.Marshal(model.TranslateRequest{Text: "Hello", TargetLang: "zh-Hant-TW"})
		require.NoError(t, err)

		msg, err := nc.Request(context.Background(), reqSubject, body, 2*time.Second)
		require.NoError(t, err)

		var res model.TranslateResult
		require.NoError(t, json.Unmarshal(msg.Data, &res))
		assert.Equal(t, "[zhTW] Hello", res.TranslatedText)
		assert.Equal(t, "zh-Hant-TW", res.TargetLang) // client value echoed, not the backend code
	})

	t.Run("validation error replies with envelope", func(t *testing.T) {
		body, err := json.Marshal(model.TranslateRequest{Text: "", TargetLang: "en"})
		require.NoError(t, err)

		msg, err := nc.Request(context.Background(), reqSubject, body, 2*time.Second)
		require.NoError(t, err)

		var env errcode.Error
		require.NoError(t, json.Unmarshal(msg.Data, &env))
		assert.Equal(t, errcode.CodeBadRequest, env.Code)
		assert.Equal(t, errcode.TranslateEmptyText, env.Reason)
	})

	t.Run("malformed payload replies with bad_request", func(t *testing.T) {
		msg, err := nc.Request(context.Background(), reqSubject, []byte("{ not json"), 2*time.Second)
		require.NoError(t, err)

		var env errcode.Error
		require.NoError(t, json.Unmarshal(msg.Data, &env))
		assert.Equal(t, errcode.CodeBadRequest, env.Code) // router rejects undecodable input before the handler
	})
}
