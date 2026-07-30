package main

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
)

func testContext() *natsrouter.Context {
	return natsrouter.NewContext(nil)
}

func TestHandler_Translate_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	tr := NewMockTranslator(ctrl)
	// Client sends the BCP-47 tag from settings; the backend receives the mapped code.
	tr.EXPECT().Translate(gomock.Any(), "Hello", "zhTW").Return("你好", nil)

	res, err := NewHandler(tr).Translate(testContext(),
		model.TranslateRequest{Text: "Hello", TargetLang: "zh-Hant-TW"})

	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, "你好", res.TranslatedText)
	assert.Equal(t, "zh-Hant-TW", res.TargetLang) // echoes the client's BCP-47 value, not the backend code
}

func TestHandler_Translate_MapsRegionTagToBaseLang(t *testing.T) {
	ctrl := gomock.NewController(t)
	tr := NewMockTranslator(ctrl)
	tr.EXPECT().Translate(gomock.Any(), "Hello", "en").Return("Hello", nil) // en-US → en

	res, err := NewHandler(tr).Translate(testContext(),
		model.TranslateRequest{Text: "Hello", TargetLang: "en-US"})

	require.NoError(t, err)
	assert.Equal(t, "en-US", res.TargetLang) // client value echoed verbatim
}

func TestHandler_Translate_ValidationErrors(t *testing.T) {
	cases := []struct {
		name       string
		text       string
		targetLang string
		wantReason errcode.Reason
	}{
		{"empty text", "", "en", errcode.TranslateEmptyText},
		{"unsupported lang", "hi", "fr", errcode.TranslateUnsupportedLang},
		{"ambiguous bare zh", "hi", "zh", errcode.TranslateUnsupportedLang}, // cannot resolve Traditional vs Simplified
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := NewHandler(mockTranslator{}).Translate(testContext(),
				model.TranslateRequest{Text: tc.text, TargetLang: tc.targetLang})

			assert.Nil(t, res)
			var ec *errcode.Error
			require.ErrorAs(t, err, &ec)
			assert.Equal(t, errcode.CodeBadRequest, ec.Code)
			assert.Equal(t, tc.wantReason, ec.Reason)
		})
	}
}

func TestHandler_Translate_BackendError(t *testing.T) {
	ctrl := gomock.NewController(t)
	tr := NewMockTranslator(ctrl)
	tr.EXPECT().Translate(gomock.Any(), "hi", "en").Return("", errors.New("upstream down"))

	res, err := NewHandler(tr).Translate(testContext(),
		model.TranslateRequest{Text: "hi", TargetLang: "en"})

	assert.Nil(t, res)
	require.Error(t, err)
	// A backend failure is returned as a raw wrapped error (not a typed errcode), so
	// it collapses to `internal` at the boundary and the cause never reaches the client.
	var ec *errcode.Error
	assert.False(t, errors.As(err, &ec), "backend error must stay untyped so it classifies to internal")
}
