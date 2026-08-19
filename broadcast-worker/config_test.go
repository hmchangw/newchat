package main

import (
	"os"
	"testing"

	"github.com/caarlos0/env/v11"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
)

func TestConfig_Mode(t *testing.T) {
	cases := []struct {
		mode    string
		want    stream.Pipeline
		wantErr bool
	}{
		{"user", stream.PipelineUser, false},
		{"bot", stream.PipelineBot, false},
		{"admin", "", true},
		{"", "", true}, // required
	}
	for _, tc := range cases {
		name := tc.mode
		if name == "" {
			name = "empty"
		}
		t.Run(name, func(t *testing.T) {
			t.Setenv("MODE", tc.mode) // pin cleanup so host MODE is restored after the test
			if tc.mode == "" {
				require.NoError(t, os.Unsetenv("MODE")) // caarlos0/env treats "" as defined; unset to test the required check
			}
			cfg, err := env.ParseAs[config]()
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, cfg.Mode)
		})
	}
}

func TestConfig_RoomSubjectMode(t *testing.T) {
	t.Setenv("MODE", "user")

	cases := []struct {
		name    string
		env     string // "" means unset — exercise envDefault
		want    subject.RoomRouteMode
		wantErr bool
	}{
		{name: "default_is_global", env: "", want: subject.RouteGlobal},
		{name: "explicit_global", env: "global", want: subject.RouteGlobal},
		{name: "dual", env: "dual", want: subject.RouteDual},
		{name: "local", env: "local", want: subject.RouteLocal},
		{name: "invalid", env: "bogus", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Seed unconditionally so t.Setenv registers a restore of any
			// inherited value; the default case unsets it below.
			t.Setenv("ROOM_SUBJECT_MODE", "global")
			if tc.env == "" {
				require.NoError(t, os.Unsetenv("ROOM_SUBJECT_MODE"))
			} else {
				t.Setenv("ROOM_SUBJECT_MODE", tc.env)
			}
			cfg, err := env.ParseAs[config]()
			require.NoError(t, err)

			mode, err := subject.ParseRoomRouteMode(cfg.RoomSubjectMode)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, mode)
		})
	}
}
