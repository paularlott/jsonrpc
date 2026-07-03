package jsonrpc_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/paularlott/jsonrpc"
)

func echoServer(t *testing.T, opts ...jsonrpc.Option) *jsonrpc.Server {
	t.Helper()
	s := jsonrpc.NewServer(opts...)
	s.Handle("echo", func(ctx context.Context, params json.RawMessage) (any, error) {
		var v any
		if err := jsonrpc.Unmarshal(params, &v); err != nil {
			return nil, err
		}
		return v, nil
	})
	return s
}

func TestHandleRequestReturnsNilForNotification(t *testing.T) {
	s := echoServer(t)
	req := &jsonrpc.Request{Method: "echo"} // no id
	if resp := s.HandleRequest(context.Background(), req); resp != nil {
		t.Fatalf("expected nil response for notification, got %+v", resp)
	}
}

func TestHandleRequestInvalidVersion(t *testing.T) {
	s := echoServer(t)
	req := &jsonrpc.Request{
		JSONRPC: json.RawMessage(`"1.0"`),
		Method:  "echo",
		ID:      json.RawMessage(`5`),
	}
	resp := s.HandleRequest(context.Background(), req)
	if resp == nil || resp.Error == nil {
		t.Fatal("expected error response")
	}
	if resp.Error.Code != jsonrpc.CodeInvalidRequest {
		t.Errorf("code = %d, want %d", resp.Error.Code, jsonrpc.CodeInvalidRequest)
	}
}

func TestHandleRequestMissingMethod(t *testing.T) {
	s := echoServer(t)
	req := &jsonrpc.Request{ID: json.RawMessage(`5`)}
	resp := s.HandleRequest(context.Background(), req)
	if resp == nil || resp.Error == nil || resp.Error.Code != jsonrpc.CodeInvalidRequest {
		t.Fatalf("expected invalid request, got %+v", resp)
	}
}

func TestPlainErrorBecomesServerError(t *testing.T) {
	s := jsonrpc.NewServer()
	s.Handle("boom", func(ctx context.Context, params json.RawMessage) (any, error) {
		return nil, errors.New("kaboom")
	})
	out, _ := call(t, s, `{"jsonrpc":"2.0","method":"boom","id":1}`)
	m := decodeObj(t, out)
	e := errObj(t, m)
	if num(e["code"]) != jsonrpc.CodeServerError {
		t.Errorf("code = %v, want %d", e["code"], jsonrpc.CodeServerError)
	}
	if !strings.Contains(e["message"].(string), "kaboom") {
		t.Errorf("message = %v, want to contain kaboom", e["message"])
	}
}

func TestCustomErrorWithData(t *testing.T) {
	s := jsonrpc.NewServer()
	s.Handle("divide", func(ctx context.Context, params json.RawMessage) (any, error) {
		return nil, jsonrpc.NewError(jsonrpc.CodeInvalidParams, "division by zero", map[string]any{"field": "b"})
	})
	out, _ := call(t, s, `{"jsonrpc":"2.0","method":"divide","id":"req-1"}`)
	m := decodeObj(t, out)
	if m["id"] != "req-1" {
		t.Errorf("id = %v, want req-1", m["id"])
	}
	e := errObj(t, m)
	if num(e["code"]) != jsonrpc.CodeInvalidParams {
		t.Errorf("code = %v, want %d", e["code"], jsonrpc.CodeInvalidParams)
	}
	data, ok := e["data"].(map[string]any)
	if !ok || data["field"] != "b" {
		t.Errorf("data = %v, want field=b", e["data"])
	}
}

func TestPanicBecomesInternalError(t *testing.T) {
	var hookMethod string
	var hookErr error
	s := jsonrpc.NewServer(jsonrpc.WithErrorHandler(func(method string, err error) {
		hookMethod, hookErr = method, err
	}))
	s.Handle("panic", func(ctx context.Context, params json.RawMessage) (any, error) {
		panic("oh no")
	})
	out, _ := call(t, s, `{"jsonrpc":"2.0","method":"panic","id":1}`)
	m := decodeObj(t, out)
	e := errObj(t, m)
	if num(e["code"]) != jsonrpc.CodeInternalError {
		t.Errorf("code = %v, want %d", e["code"], jsonrpc.CodeInternalError)
	}
	if hookMethod != "panic" || hookErr == nil {
		t.Errorf("error hook not invoked properly: method=%q err=%v", hookMethod, hookErr)
	}
}

func TestErrorHookFiresForNotificationErrors(t *testing.T) {
	var mu sync.Mutex
	var got []string
	s := jsonrpc.NewServer(jsonrpc.WithErrorHandler(func(method string, err error) {
		mu.Lock()
		got = append(got, method)
		mu.Unlock()
	}))
	s.Handle("fail", func(ctx context.Context, params json.RawMessage) (any, error) {
		return nil, errors.New("bad")
	})
	// Notification: no response, but the hook must still fire.
	if _, has := call(t, s, `{"jsonrpc":"2.0","method":"fail"}`); has {
		t.Fatal("notification should not produce a response")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 || got[0] != "fail" {
		t.Errorf("hook calls = %v, want [fail]", got)
	}
}

func TestTypedHandler(t *testing.T) {
	type addParams struct {
		A int `json:"a"`
		B int `json:"b"`
	}
	s := jsonrpc.NewServer()
	s.Handle("add", jsonrpc.Typed(func(ctx context.Context, p addParams) (any, error) {
		return p.A + p.B, nil
	}))
	out, _ := call(t, s, `{"jsonrpc":"2.0","method":"add","params":{"a":2,"b":40},"id":1}`)
	m := decodeObj(t, out)
	if num(m["result"]) != 42 {
		t.Errorf("result = %v, want 42", m["result"])
	}
}

func TestTypedHandlerInvalidParams(t *testing.T) {
	type addParams struct {
		A int `json:"a"`
	}
	s := jsonrpc.NewServer()
	s.Handle("add", jsonrpc.Typed(func(ctx context.Context, p addParams) (any, error) {
		return p.A, nil
	}))
	out, _ := call(t, s, `{"jsonrpc":"2.0","method":"add","params":"not-an-object","id":1}`)
	m := decodeObj(t, out)
	e := errObj(t, m)
	if num(e["code"]) != jsonrpc.CodeInvalidParams {
		t.Errorf("code = %v, want %d", e["code"], jsonrpc.CodeInvalidParams)
	}
}

func TestNullResultIsPresent(t *testing.T) {
	s := jsonrpc.NewServer()
	s.Handle("nothing", func(ctx context.Context, params json.RawMessage) (any, error) {
		return nil, nil
	})
	out, _ := call(t, s, `{"jsonrpc":"2.0","method":"nothing","id":1}`)
	// Per spec, a success response MUST carry a result member (null here) and
	// MUST NOT carry an error member.
	if !strings.Contains(string(out), `"result":null`) {
		t.Errorf("expected result:null in %s", out)
	}
	if strings.Contains(string(out), `"error"`) {
		t.Errorf("unexpected error member in %s", out)
	}
}

func TestRawMessageResultPassthrough(t *testing.T) {
	s := jsonrpc.NewServer()
	s.Handle("raw", func(ctx context.Context, params json.RawMessage) (any, error) {
		return json.RawMessage(`{"pre":"encoded"}`), nil
	})
	out, _ := call(t, s, `{"jsonrpc":"2.0","method":"raw","id":1}`)
	m := decodeObj(t, out)
	res, ok := m["result"].(map[string]any)
	if !ok || res["pre"] != "encoded" {
		t.Errorf("result = %v, want {pre:encoded}", m["result"])
	}
}

func TestUnregister(t *testing.T) {
	s := echoServer(t)
	if !s.Unregister("echo") {
		t.Fatal("expected echo to be registered")
	}
	if s.Unregister("echo") {
		t.Fatal("expected echo to be gone")
	}
	out, _ := call(t, s, `{"jsonrpc":"2.0","method":"echo","id":1}`)
	m := decodeObj(t, out)
	e := errObj(t, m)
	if num(e["code"]) != jsonrpc.CodeMethodNotFound {
		t.Errorf("code = %v, want method not found", e["code"])
	}
}

func TestMethods(t *testing.T) {
	s := jsonrpc.NewServer()
	s.Handle("a", func(context.Context, json.RawMessage) (any, error) { return nil, nil })
	s.Handle("b", func(context.Context, json.RawMessage) (any, error) { return nil, nil })
	names := s.Methods()
	if len(names) != 2 {
		t.Fatalf("methods = %v, want 2", names)
	}
}

func TestHandleNilHandlerPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on nil handler")
		}
	}()
	jsonrpc.NewServer().Handle("x", nil)
}

func TestIDShapePreserved(t *testing.T) {
	s := echoServer(t)
	cases := []struct {
		id     string
		expect any
	}{
		{`1`, float64(1)},
		{`"abc"`, "abc"},
		{`123456789012345`, float64(123456789012345)},
	}
	for _, tc := range cases {
		in := `{"jsonrpc":"2.0","method":"echo","params":1,"id":` + tc.id + `}`
		out, _ := call(t, s, in)
		m := decodeObj(t, out)
		switch want := tc.expect.(type) {
		case string:
			if m["id"] != want {
				t.Errorf("id %s -> %v, want %v", tc.id, m["id"], want)
			}
		case float64:
			if num(m["id"]) != want {
				t.Errorf("id %s -> %v, want %v", tc.id, m["id"], want)
			}
		}
	}
}
