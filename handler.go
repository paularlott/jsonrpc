package jsonrpc

import (
	"context"
	"encoding/json"
)

// Handler processes a single JSON-RPC method call.
//
// params carries the raw JSON of the request's params member (nil when absent),
// letting the handler decode into whatever type it wants — use [Unmarshal] or
// json.Unmarshal directly. The returned value is marshalled into the response
// result. To signal a JSON-RPC error, return a *[Error] (its code, message and
// data are used verbatim); any other non-nil error becomes a [CodeServerError].
//
// The same handler serves both requests and notifications. When invoked for a
// notification the return value and error are discarded (no response is sent),
// though a returned error is still reported to an [Options.OnError] hook if set.
type Handler func(ctx context.Context, params json.RawMessage) (any, error)

// Unmarshal decodes raw params into v. It is a thin convenience wrapper over
// json.Unmarshal that treats absent (empty) params as a no-op, leaving v at its
// zero value. Returns a [CodeInvalidParams] [Error] on failure so it can be
// returned directly from a [Handler].
func Unmarshal(params json.RawMessage, v any) error {
	if len(params) == 0 {
		return nil
	}
	if err := json.Unmarshal(params, v); err != nil {
		return InvalidParams(err.Error())
	}
	return nil
}

// Typed adapts a strongly-typed handler into a [Handler]. The params are
// decoded into a fresh T before fn is called; a decode failure is reported as a
// [CodeInvalidParams] error without invoking fn.
//
//	srv.Handle("add", jsonrpc.Typed(func(ctx context.Context, p AddParams) (any, error) {
//	    return p.A + p.B, nil
//	}))
func Typed[T any](fn func(ctx context.Context, params T) (any, error)) Handler {
	return func(ctx context.Context, raw json.RawMessage) (any, error) {
		var params T
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &params); err != nil {
				return nil, InvalidParams(err.Error())
			}
		}
		return fn(ctx, params)
	}
}
