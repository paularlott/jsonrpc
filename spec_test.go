package jsonrpc_test

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/paularlott/jsonrpc"
)

// subtractServer builds a server implementing the "subtract" method from the
// JSON-RPC 2.0 specification examples, accepting both positional ([minuend,
// subtrahend]) and named ({"minuend":..,"subtrahend":..}) params.
func subtractServer(t *testing.T) *jsonrpc.Server {
	t.Helper()
	s := jsonrpc.NewServer(jsonrpc.WithSerialDispatch())
	s.Handle("subtract", func(ctx context.Context, params json.RawMessage) (any, error) {
		var positional []json.Number
		if err := json.Unmarshal(params, &positional); err == nil {
			if len(positional) != 2 {
				return nil, jsonrpc.InvalidParams("subtract needs two arguments")
			}
			a, _ := positional[0].Float64()
			b, _ := positional[1].Float64()
			return a - b, nil
		}
		var named struct {
			Minuend    float64 `json:"minuend"`
			Subtrahend float64 `json:"subtrahend"`
		}
		if err := json.Unmarshal(params, &named); err == nil {
			return named.Minuend - named.Subtrahend, nil
		}
		return nil, jsonrpc.InvalidParams("subtract: bad params")
	})
	s.Handle("update", func(ctx context.Context, params json.RawMessage) (any, error) {
		return nil, nil
	})
	s.Handle("foobar", func(ctx context.Context, params json.RawMessage) (any, error) {
		return nil, nil
	})
	s.Handle("sum", func(ctx context.Context, params json.RawMessage) (any, error) {
		var nums []float64
		if err := jsonrpc.Unmarshal(params, &nums); err != nil {
			return nil, err
		}
		var total float64
		for _, n := range nums {
			total += n
		}
		return total, nil
	})
	s.Handle("get_data", func(ctx context.Context, params json.RawMessage) (any, error) {
		return []any{"hello", 5}, nil
	})
	s.Handle("notify_hello", func(ctx context.Context, params json.RawMessage) (any, error) {
		return nil, nil
	})
	return s
}

// call feeds a raw message to the server and returns the decoded output plus
// whether a response was produced.
func call(t *testing.T, s *jsonrpc.Server, in string) (json.RawMessage, bool) {
	t.Helper()
	out, has := s.HandleMessage(context.Background(), []byte(in))
	return out, has
}

func decodeObj(t *testing.T, b []byte) map[string]any {
	t.Helper()
	var m map[string]any
	dec := json.NewDecoder(newReader(b))
	dec.UseNumber()
	if err := dec.Decode(&m); err != nil {
		t.Fatalf("decode object: %v (raw=%s)", err, b)
	}
	return m
}

func decodeArr(t *testing.T, b []byte) []map[string]any {
	t.Helper()
	var a []map[string]any
	dec := json.NewDecoder(newReader(b))
	dec.UseNumber()
	if err := dec.Decode(&a); err != nil {
		t.Fatalf("decode array: %v (raw=%s)", err, b)
	}
	return a
}

// Spec example: rpc call with positional parameters.
func TestSpecPositionalParams(t *testing.T) {
	s := subtractServer(t)
	out, has := call(t, s, `{"jsonrpc": "2.0", "method": "subtract", "params": [42, 23], "id": 1}`)
	if !has {
		t.Fatal("expected a response")
	}
	m := decodeObj(t, out)
	if m["jsonrpc"] != "2.0" {
		t.Errorf("jsonrpc = %v", m["jsonrpc"])
	}
	if num(m["result"]) != 19 {
		t.Errorf("result = %v, want 19", m["result"])
	}
	if num(m["id"]) != 1 {
		t.Errorf("id = %v, want 1", m["id"])
	}
	if _, ok := m["error"]; ok {
		t.Errorf("unexpected error member")
	}
}

// Spec example: rpc call with named parameters.
func TestSpecNamedParams(t *testing.T) {
	s := subtractServer(t)
	out, _ := call(t, s, `{"jsonrpc": "2.0", "method": "subtract", "params": {"subtrahend": 23, "minuend": 42}, "id": 3}`)
	m := decodeObj(t, out)
	if num(m["result"]) != 19 {
		t.Errorf("result = %v, want 19", m["result"])
	}
	if num(m["id"]) != 3 {
		t.Errorf("id = %v, want 3", m["id"])
	}
}

// Spec example: a notification produces no response.
func TestSpecNotification(t *testing.T) {
	s := subtractServer(t)
	if out, has := call(t, s, `{"jsonrpc": "2.0", "method": "update", "params": [1,2,3,4,5]}`); has {
		t.Errorf("notification produced a response: %s", out)
	}
	if out, has := call(t, s, `{"jsonrpc": "2.0", "method": "foobar"}`); has {
		t.Errorf("notification produced a response: %s", out)
	}
}

// Spec example: rpc call of non-existent method.
func TestSpecMethodNotFound(t *testing.T) {
	s := subtractServer(t)
	out, _ := call(t, s, `{"jsonrpc": "2.0", "method": "foobar_missing", "id": "1"}`)
	m := decodeObj(t, out)
	e := errObj(t, m)
	if num(e["code"]) != jsonrpc.CodeMethodNotFound {
		t.Errorf("code = %v, want %d", e["code"], jsonrpc.CodeMethodNotFound)
	}
	if m["id"] != "1" {
		t.Errorf("id = %v, want string \"1\"", m["id"])
	}
}

// Spec example: rpc call with invalid JSON.
func TestSpecInvalidJSON(t *testing.T) {
	s := subtractServer(t)
	out, has := call(t, s, `{"jsonrpc": "2.0", "method": "foobar, "params": "bar", "baz]`)
	if !has {
		t.Fatal("expected a parse error response")
	}
	m := decodeObj(t, out)
	e := errObj(t, m)
	if num(e["code"]) != jsonrpc.CodeParseError {
		t.Errorf("code = %v, want %d", e["code"], jsonrpc.CodeParseError)
	}
	if m["id"] != nil {
		t.Errorf("id = %v, want null", m["id"])
	}
}

// Spec example: rpc call with an invalid Request object.
func TestSpecInvalidRequestObject(t *testing.T) {
	s := subtractServer(t)
	out, _ := call(t, s, `{"jsonrpc": "2.0", "method": 1, "params": "bar"}`)
	m := decodeObj(t, out)
	e := errObj(t, m)
	if num(e["code"]) != jsonrpc.CodeInvalidRequest {
		t.Errorf("code = %v, want %d", e["code"], jsonrpc.CodeInvalidRequest)
	}
	if m["id"] != nil {
		t.Errorf("id = %v, want null", m["id"])
	}
}

// Spec example: rpc call Batch, invalid JSON.
func TestSpecBatchInvalidJSON(t *testing.T) {
	s := subtractServer(t)
	out, has := call(t, s, `[ {"jsonrpc": "2.0", "method": "sum", "params": [1,2,4], "id": "1"}, {"jsonrpc": "2.0", "method" ]`)
	if !has {
		t.Fatal("expected a parse error response")
	}
	m := decodeObj(t, out)
	e := errObj(t, m)
	if num(e["code"]) != jsonrpc.CodeParseError {
		t.Errorf("code = %v, want %d", e["code"], jsonrpc.CodeParseError)
	}
}

// Spec example: rpc call with an empty Array.
func TestSpecEmptyBatch(t *testing.T) {
	s := subtractServer(t)
	out, _ := call(t, s, `[]`)
	m := decodeObj(t, out)
	e := errObj(t, m)
	if num(e["code"]) != jsonrpc.CodeInvalidRequest {
		t.Errorf("code = %v, want %d", e["code"], jsonrpc.CodeInvalidRequest)
	}
}

// Spec example: rpc call with an invalid Batch (but not empty).
func TestSpecInvalidBatchNotEmpty(t *testing.T) {
	s := subtractServer(t)
	out, _ := call(t, s, `[1]`)
	arr := decodeArr(t, out)
	if len(arr) != 1 {
		t.Fatalf("expected 1 response, got %d", len(arr))
	}
	e := errObj(t, arr[0])
	if num(e["code"]) != jsonrpc.CodeInvalidRequest {
		t.Errorf("code = %v, want %d", e["code"], jsonrpc.CodeInvalidRequest)
	}
	if arr[0]["id"] != nil {
		t.Errorf("id = %v, want null", arr[0]["id"])
	}
}

// Spec example: rpc call with invalid Batch.
func TestSpecInvalidBatch(t *testing.T) {
	s := subtractServer(t)
	out, _ := call(t, s, `[1,2,3]`)
	arr := decodeArr(t, out)
	if len(arr) != 3 {
		t.Fatalf("expected 3 responses, got %d", len(arr))
	}
	for i, r := range arr {
		e := errObj(t, r)
		if num(e["code"]) != jsonrpc.CodeInvalidRequest {
			t.Errorf("response %d code = %v, want %d", i, e["code"], jsonrpc.CodeInvalidRequest)
		}
	}
}

// Spec example: rpc call Batch (mix of calls and notifications).
func TestSpecBatchMixed(t *testing.T) {
	s := subtractServer(t)
	in := `[
        {"jsonrpc": "2.0", "method": "sum", "params": [1,2,4], "id": "1"},
        {"jsonrpc": "2.0", "method": "notify_hello", "params": [7]},
        {"jsonrpc": "2.0", "method": "subtract", "params": [42,23], "id": "2"},
        {"foo": "boo"},
        {"jsonrpc": "2.0", "method": "foo.get", "params": {"name": "myself"}, "id": "5"},
        {"jsonrpc": "2.0", "method": "get_data", "id": "9"}
    ]`
	out, _ := call(t, s, in)
	arr := decodeArr(t, out)
	// 6 elements in; one is a notification (no response) => 5 responses.
	if len(arr) != 5 {
		t.Fatalf("expected 5 responses, got %d: %s", len(arr), out)
	}

	byID := map[string]map[string]any{}
	var nullIDCount int
	for _, r := range arr {
		if r["id"] == nil {
			nullIDCount++
			continue
		}
		byID[r["id"].(string)] = r
	}

	if num(byID["1"]["result"]) != 7 {
		t.Errorf("sum result = %v, want 7", byID["1"]["result"])
	}
	if num(byID["2"]["result"]) != 19 {
		t.Errorf("subtract result = %v, want 19", byID["2"]["result"])
	}
	// {"foo":"boo"} -> InvalidRequest with null id.
	if nullIDCount != 1 {
		t.Errorf("expected 1 null-id error response, got %d", nullIDCount)
	}
	// foo.get -> method not found.
	if e := errObj(t, byID["5"]); num(e["code"]) != jsonrpc.CodeMethodNotFound {
		t.Errorf("foo.get code = %v, want %d", e["code"], jsonrpc.CodeMethodNotFound)
	}
	// get_data -> ["hello", 5].
	res, ok := byID["9"]["result"].([]any)
	if !ok || len(res) != 2 || res[0] != "hello" || num(res[1]) != 5 {
		t.Errorf("get_data result = %v, want [hello 5]", byID["9"]["result"])
	}
}

// Spec example: rpc call Batch where all elements are notifications.
func TestSpecBatchAllNotifications(t *testing.T) {
	s := subtractServer(t)
	in := `[
        {"jsonrpc": "2.0", "method": "notify_hello", "params": [1,2,4]},
        {"jsonrpc": "2.0", "method": "notify_hello", "params": [7]}
    ]`
	if out, has := call(t, s, in); has {
		t.Errorf("all-notification batch produced output: %s", out)
	}
}

// num converts a decoded JSON number (json.Number) into a float64 for assertions.
func num(v any) float64 {
	switch n := v.(type) {
	case json.Number:
		f, _ := n.Float64()
		return f
	case float64:
		return n
	case int:
		return float64(n)
	}
	return 0
}

func errObj(t *testing.T, m map[string]any) map[string]any {
	t.Helper()
	e, ok := m["error"].(map[string]any)
	if !ok {
		t.Fatalf("expected error object, got %#v", m)
	}
	return e
}

// ensure reflect import is used even if assertions change over time.
var _ = reflect.DeepEqual
