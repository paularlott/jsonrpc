package jsonrpc_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/paularlott/jsonrpc"
)

func TestIsNotification(t *testing.T) {
	var sawRequest, sawNotification bool
	s := jsonrpc.NewServer(jsonrpc.WithSerialDispatch())
	s.Handle("probe", func(ctx context.Context, params json.RawMessage) (any, error) {
		if jsonrpc.IsNotification(ctx) {
			sawNotification = true
		} else {
			sawRequest = true
		}
		return "ok", nil
	})

	// Request form: id present.
	if _, has := s.HandleMessage(context.Background(), []byte(`{"jsonrpc":"2.0","method":"probe","id":1}`)); !has {
		t.Fatal("expected a response for the request")
	}
	// Notification form: no id.
	if _, has := s.HandleMessage(context.Background(), []byte(`{"jsonrpc":"2.0","method":"probe"}`)); has {
		t.Fatal("did not expect a response for the notification")
	}

	if !sawRequest {
		t.Error("handler never observed a request call")
	}
	if !sawNotification {
		t.Error("handler never observed a notification call")
	}
}

func TestIsNotificationFalseOutsideHandler(t *testing.T) {
	if jsonrpc.IsNotification(context.Background()) {
		t.Error("IsNotification should be false outside a handler")
	}
}

// TestNotificationOnlyRoutingPattern documents the pattern scriptling uses to
// preserve method/notification split with a single registry: reject a
// request-form call for a notification-only name.
func TestNotificationOnlyRoutingPattern(t *testing.T) {
	s := jsonrpc.NewServer()
	s.Handle("event", func(ctx context.Context, params json.RawMessage) (any, error) {
		if !jsonrpc.IsNotification(ctx) {
			return nil, jsonrpc.MethodNotFound("method not found: event")
		}
		return nil, nil
	})

	// Request-form call must be rejected as method-not-found.
	out, has := s.HandleMessage(context.Background(), []byte(`{"jsonrpc":"2.0","method":"event","id":1}`))
	if !has {
		t.Fatal("expected an error response")
	}
	var resp map[string]any
	json.Unmarshal(out, &resp)
	e := resp["error"].(map[string]any)
	if int(e["code"].(float64)) != jsonrpc.CodeMethodNotFound {
		t.Errorf("code = %v, want %d", e["code"], jsonrpc.CodeMethodNotFound)
	}

	// Notification-form call is accepted, no response.
	if _, has := s.HandleMessage(context.Background(), []byte(`{"jsonrpc":"2.0","method":"event"}`)); has {
		t.Fatal("notification should produce no response")
	}
}
