//go:build integration

package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }

// Drives the handler in-process while publishing the result over a real NATS
// connection, verifying the TranslateResult round-trips on the response subject.
func TestTranslate_EndToEnd(t *testing.T) {
	url := testutil.NATS(t)
	nc, err := nats.Connect(url)
	require.NoError(t, err)
	t.Cleanup(func() { _ = nc.Drain() })

	sub, err := nc.SubscribeSync(subject.UserResponse("alice", "req-e2e"))
	require.NoError(t, err)

	h := NewHandler(mockTranslator{}, func(_ context.Context, subj string, data []byte) error {
		return nc.Publish(subj, data)
	})
	c := natsrouter.NewContext(map[string]string{"account": "alice"})
	// BCP-47 targetLang from settings; the mock backend receives the mapped code (zhTW).
	require.NoError(t, h.Translate(c, model.TranslateRequest{RequestID: "req-e2e", Text: "Hello", TargetLang: "zh-Hant-TW"}))

	msg, err := sub.NextMsg(2 * time.Second)
	require.NoError(t, err)
	var res model.TranslateResult
	require.NoError(t, json.Unmarshal(msg.Data, &res))
	require.Equal(t, model.TranslateStatusOK, res.Status)
	require.Equal(t, "[zhTW] Hello", res.TranslatedText)
	require.Equal(t, "zh-Hant-TW", res.TargetLang) // client value echoed, not the backend code
	require.Equal(t, "req-e2e", res.RequestID)
}
