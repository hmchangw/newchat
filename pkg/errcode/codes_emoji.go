package errcode

// Reasons emitted by media-service custom-emoji endpoints.
const (
	// EmojiShortcodeReserved signals the shortcode collides with a built-in
	// standard emoji: the reaction validator resolves standard names first, so
	// a custom emoji under that name could never be used.
	EmojiShortcodeReserved Reason = "emoji_shortcode_reserved"

	// EmojiDeleteDisabled signals the emoji.delete RPC is switched off via
	// EMOJI_DELETE_ENABLED=false (kill-switch, default off in v1).
	EmojiDeleteDisabled Reason = "emoji_delete_disabled"
)
