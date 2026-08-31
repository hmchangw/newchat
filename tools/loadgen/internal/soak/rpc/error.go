package rpc

import "errors"

// soakRequestError carries the identity of a failed RPC alongside its cause.
// A metric spike names the action and nothing else, so without these fields on
// the log line there is nothing to grep the server's logs for.
type soakRequestError struct {
	Action   soakRPCAction
	Subject  string
	Account  string
	RoomID   string
	Class    soakErrorClass
	Reason   soakErrorReason
	Attempts int
	Retries  int
	Cause    error
}

// Error passes the cause through unchanged. The action reaches the reader
// twice over already — once from the lane's own wrap, once from the action
// attr — and a third copy here only lengthens the line.
func (e *soakRequestError) Error() string {
	if e.Cause == nil {
		return string(e.Action) + " request failed"
	}
	return e.Cause.Error()
}

// Unwrap keeps errors.Is working through the carrier: retry classification and
// the ledger both key on sentinels below it.
func (e *soakRequestError) Unwrap() error { return e.Cause }

// soakErrorAttrs renders err as slog key/value pairs, adding the request
// identity when a carrier is anywhere in the chain. Empty fields are dropped —
// a room_id="" on every user-read error trains readers to skip the field.
func soakErrorAttrs(err error) []any {
	if err == nil {
		return nil
	}
	attrs := []any{"error", err}
	var carrier *soakRequestError
	if !errors.As(err, &carrier) {
		return attrs
	}
	if carrier.Action != "" {
		attrs = append(attrs, "action", string(carrier.Action))
	}
	if carrier.Account != "" {
		attrs = append(attrs, "account", carrier.Account)
	}
	if carrier.RoomID != "" {
		attrs = append(attrs, "room_id", carrier.RoomID)
	}
	if carrier.Subject != "" {
		attrs = append(attrs, "subject", carrier.Subject)
	}
	if carrier.Class != "" {
		attrs = append(attrs, "error_class", string(carrier.Class))
	}
	if carrier.Reason != "" {
		attrs = append(attrs, "error_reason", string(carrier.Reason))
	}
	if carrier.Attempts > 0 {
		attrs = append(attrs, "attempts", carrier.Attempts)
	}
	if carrier.Retries > 0 {
		attrs = append(attrs, "retries", carrier.Retries)
	}
	return attrs
}

type RequestError = soakRequestError

func ErrorAttrs(err error) []any {
	return soakErrorAttrs(err)
}
