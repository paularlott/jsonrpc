package jsonrpc

import "context"

// contextKey is the unexported type for context keys defined in this package,
// avoiding collisions with keys from other packages.
type contextKey int

const notificationKey contextKey = iota

// withNotification returns a copy of ctx that records whether the current call
// is a notification.
func withNotification(ctx context.Context, notification bool) context.Context {
	return context.WithValue(ctx, notificationKey, notification)
}

// IsNotification reports whether the in-flight call is a JSON-RPC notification
// (a request without an id, for which no response is sent). It is valid only
// inside a [Handler]; it returns false elsewhere.
//
// The same handler serves both requests and notifications, so this lets a
// handler adjust its behaviour for the fire-and-forget case — for example to
// reject notification-only or request-only methods.
func IsNotification(ctx context.Context) bool {
	v, _ := ctx.Value(notificationKey).(bool)
	return v
}
