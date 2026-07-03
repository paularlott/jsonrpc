package jsonrpc_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/paularlott/jsonrpc"
)

func TestServeHTTPSingle(t *testing.T) {
	s := echoServer(t)
	srv := httptest.NewServer(s)
	defer srv.Close()

	resp, err := http.Post(srv.URL, "application/json", strings.NewReader(`{"jsonrpc":"2.0","method":"echo","params":{"hi":"there"},"id":1}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Errorf("content-type = %q", ct)
	}
	var m map[string]any
	json.NewDecoder(resp.Body).Decode(&m)
	res := m["result"].(map[string]any)
	if res["hi"] != "there" {
		t.Errorf("result = %v", m["result"])
	}
}

func TestServeHTTPNotification204(t *testing.T) {
	s := echoServer(t)
	srv := httptest.NewServer(s)
	defer srv.Close()

	resp, err := http.Post(srv.URL, "application/json", strings.NewReader(`{"jsonrpc":"2.0","method":"echo","params":1}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if len(body) != 0 {
		t.Errorf("body = %q, want empty", body)
	}
}

func TestServeHTTPRejectsNonPOST(t *testing.T) {
	s := echoServer(t)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
	if rec.Header().Get("Allow") != http.MethodPost {
		t.Errorf("Allow = %q", rec.Header().Get("Allow"))
	}
}

func TestServeHTTPBatch(t *testing.T) {
	s := subtractServer(t)
	srv := httptest.NewServer(s)
	defer srv.Close()

	body := `[
        {"jsonrpc":"2.0","method":"subtract","params":[42,23],"id":1},
        {"jsonrpc":"2.0","method":"notify_hello","params":[1]},
        {"jsonrpc":"2.0","method":"subtract","params":[10,1],"id":2}
    ]`
	resp, err := http.Post(srv.URL, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var arr []map[string]any
	json.NewDecoder(resp.Body).Decode(&arr)
	if len(arr) != 2 {
		t.Fatalf("expected 2 responses, got %d", len(arr))
	}
}

func TestServeHTTPEmptyBody(t *testing.T) {
	s := echoServer(t)
	srv := httptest.NewServer(s)
	defer srv.Close()

	resp, err := http.Post(srv.URL, "application/json", strings.NewReader(""))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var m map[string]any
	json.NewDecoder(resp.Body).Decode(&m)
	e := m["error"].(map[string]any)
	if num(e["code"]) != jsonrpc.CodeParseError {
		t.Errorf("code = %v, want parse error", e["code"])
	}
}

// End-to-end: Client over HTTPTransport against a real server.
func TestClientOverHTTP(t *testing.T) {
	type addParams struct {
		A, B int
	}
	s := jsonrpc.NewServer()
	s.Handle("add", jsonrpc.Typed(func(ctx context.Context, p addParams) (any, error) {
		return p.A + p.B, nil
	}))
	s.Handle("fail", func(ctx context.Context, params json.RawMessage) (any, error) {
		return nil, jsonrpc.NewError(-32001, "nope", nil)
	})
	srv := httptest.NewServer(s)
	defer srv.Close()

	client := jsonrpc.NewClient(jsonrpc.NewHTTPTransport(srv.URL))
	defer client.Close()

	var sum int
	if err := client.Call(context.Background(), "add", addParams{A: 40, B: 2}, &sum); err != nil {
		t.Fatalf("call: %v", err)
	}
	if sum != 42 {
		t.Errorf("sum = %d, want 42", sum)
	}

	// Error propagation as *jsonrpc.Error.
	err := client.Call(context.Background(), "fail", nil, nil)
	var rpcErr *jsonrpc.Error
	if !asErr(err, &rpcErr) {
		t.Fatalf("expected *jsonrpc.Error, got %T: %v", err, err)
	}
	if rpcErr.Code != -32001 {
		t.Errorf("code = %d, want -32001", rpcErr.Code)
	}

	// Notification over HTTP.
	if err := client.Notify(context.Background(), "add", addParams{A: 1, B: 1}); err != nil {
		t.Errorf("notify: %v", err)
	}
}

func TestHTTPTransportHeader(t *testing.T) {
	var gotAuth string
	s := jsonrpc.NewServer()
	s.Handle("ping", func(ctx context.Context, params json.RawMessage) (any, error) {
		return "pong", nil
	})
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		s.ServeHTTP(w, r)
	})
	srv := httptest.NewServer(handler)
	defer srv.Close()

	client := jsonrpc.NewClient(jsonrpc.NewHTTPTransport(srv.URL, jsonrpc.WithHeader("Authorization", "Bearer xyz")))
	var out string
	if err := client.Call(context.Background(), "ping", nil, &out); err != nil {
		t.Fatal(err)
	}
	if out != "pong" {
		t.Errorf("out = %q", out)
	}
	if gotAuth != "Bearer xyz" {
		t.Errorf("auth header = %q, want Bearer xyz", gotAuth)
	}
}

// asErr is a tiny errors.As shim to avoid importing errors in every test.
func asErr(err error, target **jsonrpc.Error) bool {
	e, ok := err.(*jsonrpc.Error)
	if ok {
		*target = e
	}
	return ok
}
