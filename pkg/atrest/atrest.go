// Package atrest provides envelope encryption of message payloads at rest.
//
// Each room owns a single 256-bit Data Encryption Key (DEK) used with
// AES-256-GCM to encrypt a JSON-serialised payload. Each DEK is itself
// wrapped by a KeyWrapper backed by Vault's transit secrets engine —
// the plaintext KEK never leaves the Vault server. The wrapped DEK is
// stored in MongoDB; only the unwrapped form is held in process memory
// (in a bounded LRU cache).
package atrest

import (
	"errors"
	"time"

	"github.com/hmchangw/chat/pkg/model/cassandra"
)

// EncryptedFields is the bundle of user-authored content that gets
// serialised, encrypted and stored in the Cassandra `enc_payload` column.
// Field names mirror the plaintext columns so that callers can construct
// this struct directly from a cassandra.Message.
//
// sys_msg_data is intentionally NOT encrypted — it carries system-generated
// metadata (e.g. the room members being added), not user-authored secrets,
// so it stays in its plaintext column.
type EncryptedFields struct {
	Msg                 string                 `json:"msg,omitempty"`
	Attachments         [][]byte               `json:"attachments,omitempty"`
	Card                *cassandra.Card        `json:"card,omitempty"`
	CardAction          *cassandra.CardAction  `json:"cardAction,omitempty"`
	QuotedParentContent *QuotedParentEncrypted `json:"quotedParentContent,omitempty"`
}

// QuotedParentEncrypted holds the user-authored fields of a quoted parent
// message. Mentions, sender, timestamps and IDs stay plaintext on the
// quoted_parent_message UDT.
type QuotedParentEncrypted struct {
	Msg         string   `json:"msg,omitempty"`
	Attachments [][]byte `json:"attachments,omitempty"`
}

// EncMeta is the per-row metadata stored alongside the ciphertext.
// This is the crypto-API form. The cql-tagged sibling for gocql binding
// lives in pkg/model/cassandra; the two are converted via a one-line
// struct literal at service boundaries.
type EncMeta struct {
	Nonce []byte `json:"nonce"`
}

// RoomDataKey is the wrapped DEK record stored in MongoDB. WrappedDEK is
// the opaque ciphertext returned by the configured KeyWrapper (for the
// Vault transit engine, this is a "vault:vN:..." string carrying its own
// version metadata). No KEK version or wrap nonce is stored on the row —
// both are encoded inside WrappedDEK.
type RoomDataKey struct {
	ID         string    `bson:"_id"`
	WrappedDEK []byte    `bson:"wrappedDEK"`
	CreatedAt  time.Time `bson:"createdAt"`
}

// Config is parsed via caarlos0/env in each consuming service. It is the
// shared transport-agnostic config; Vault-specific settings live in
// VaultConfig.
type Config struct {
	Enabled      bool          `env:"ATREST_ENABLED"        envDefault:"true"`
	DEKCacheSize int           `env:"ATREST_DEK_CACHE_SIZE" envDefault:"10000"`
	DEKCacheTTL  time.Duration `env:"ATREST_DEK_CACHE_TTL"  envDefault:"1h"`
}

// Sentinel errors. Callers use errors.Is to identify a class.
var (
	// ErrAuthFailed means the GCM authentication tag did not validate
	// during decrypt — the ciphertext was tampered with or the wrong key
	// was used.
	ErrAuthFailed = errors.New("atrest: authentication failed")
	// ErrPayloadMalformed means JSON unmarshal of the decrypted payload
	// failed.
	ErrPayloadMalformed = errors.New("atrest: payload malformed")
)
