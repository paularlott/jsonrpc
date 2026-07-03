package jsonrpc_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/paularlott/jsonrpc"
)

func batchTestServer() *jsonrpc.Server {
	s := jsonrpc.NewServer()
	s.Handle("add", jsonrpc.Typed(func(ctx context.Context, p struct{ A, B int }) (any, error) {
		return p.A + p.B, nil
	}))
	s.Handle("fail", func(ctx context.Context, params json.RawMessage) (any, error) {
		return nil, jsonrpc.NewError(-32001, "always fails", nil)
	})
	s.Handle("echo", func(ctx context.Context, params json.RawMessage) (any, error) {
		return params, nil
	})
	s.Handle("slow", func(ctx context.Context, params json.RawMessage) (any, error) {
		time.Sleep(30 * time.Millisecond)
		return "done", nil
	})
	return s
}

// TestCallBatchOverHTTP verifies results come back in call order and that a
// per-call error doesn't affect sibling calls.
func TestCallBatchOverHTTP(t *testing.T) {
	srv := httptest.NewServer(batchTestServer())
	defer srv.Close()

	client := jsonrpc.NewClient(jsonrpc.NewHTTPTransport(srv.URL))
	defer client.Close()

	var sum1, sum2 int
	results := client.CallBatch(context.Background(), []jsonrpc.BatchCall{
		{Method: "add", Params: map[string]int{"A": 1, "B": 2}, Out: &sum1},
		{Method: "fail"},
		{Method: "add", Params: map[string]int{"A": 10, "B": 20}, Out: &sum2},
	})

	if len(results) != 3 {
		t.Fatalf("len(results) = %d, want 3", len(results))
	}
	if results[0].Err != nil {
		t.Errorf("results[0].Err = %v", results[0].Err)
	}
	if sum1 != 3 {
		t.Errorf("sum1 = %d, want 3", sum1)
	}
	var rpcErr *jsonrpc.Error
	if !errors.As(results[1].Err, &rpcErr) || rpcErr.Code != -32001 {
		t.Errorf("results[1].Err = %v, want *Error code -32001", results[1].Err)
	}
	if results[2].Err != nil {
		t.Errorf("results[2].Err = %v", results[2].Err)
	}
	if sum2 != 30 {
		t.Errorf("sum2 = %d, want 30", sum2)
	}
}

// TestCallBatchIsSingleHTTPRequest confirms CallBatch sends exactly one HTTP
// request carrying a JSON array body, not one request per call.
func TestCallBatchIsSingleHTTPRequest(t *testing.T) {
	real := batchTestServer()
	var requestCount atomic.Int32
	var sawArray atomic.Bool

	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
		body, _ := io.ReadAll(r.Body)
		sawArray.Store(strings.HasPrefix(strings.TrimSpace(string(body)), "["))
		r.Body = io.NopCloser(bytes.NewReader(body))
		real.ServeHTTP(w, r)
	}))
	defer proxy.Close()

	client := jsonrpc.NewClient(jsonrpc.NewHTTPTransport(proxy.URL))
	defer client.Close()

	results := client.CallBatch(context.Background(), []jsonrpc.BatchCall{
		{Method: "add", Params: map[string]int{"A": 1, "B": 1}},
		{Method: "add", Params: map[string]int{"A": 2, "B": 2}},
		{Method: "add", Params: map[string]int{"A": 3, "B": 3}},
	})
	for i, r := range results {
		if r.Err != nil {
			t.Errorf("results[%d].Err = %v", i, r.Err)
		}
	}
	if requestCount.Load() != 1 {
		t.Errorf("HTTP requests = %d, want 1", requestCount.Load())
	}
	if !sawArray.Load() {
		t.Error("expected a single JSON array batch body")
	}
}

// TestCallBatchOverStream verifies batch calls over the stream transport (the
// transport backing stdio and subprocess clients) using an in-process pipe.
func TestCallBatchOverStream(t *testing.T) {
	client, cleanup := pipeClientServer(t, batchTestServer())
	defer cleanup()

	var echoed string
	results := client.CallBatch(context.Background(), []jsonrpc.BatchCall{
		{Method: "echo", Params: "hello", Out: &echoed},
		{Method: "fail"},
	})
	if len(results) != 2 {
		t.Fatalf("len(results) = %d, want 2", len(results))
	}
	if results[0].Err != nil {
		t.Errorf("results[0].Err = %v", results[0].Err)
	}
	if echoed != "hello" {
		t.Errorf("echoed = %q, want hello", echoed)
	}
	var rpcErr *jsonrpc.Error
	if !errors.As(results[1].Err, &rpcErr) || rpcErr.Code != -32001 {
		t.Errorf("results[1].Err = %v, want *Error code -32001", results[1].Err)
	}
}

// TestCallBatchOverStreamConcurrentOnServer proves the batch is handled as one
// wire message dispatched concurrently server-side: N "slow" calls finish in
// roughly one call's duration, not the serial sum.
func TestCallBatchOverStreamConcurrentOnServer(t *testing.T) {
	client, cleanup := pipeClientServer(t, batchTestServer())
	defer cleanup()

	calls := make([]jsonrpc.BatchCall, 5)
	for i := range calls {
		calls[i] = jsonrpc.BatchCall{Method: "slow"}
	}

	start := time.Now()
	results := client.CallBatch(context.Background(), calls)
	elapsed := time.Since(start)

	for i, r := range results {
		if r.Err != nil {
			t.Errorf("results[%d].Err = %v", i, r.Err)
		}
	}
	if elapsed > 150*time.Millisecond {
		t.Errorf("expected concurrent server-side dispatch (<150ms), took %v", elapsed)
	}
}

// TestCallBatchEmpty returns a non-nil empty slice and performs no I/O.
func TestCallBatchEmpty(t *testing.T) {
	client := jsonrpc.NewClient(jsonrpc.NewHTTPTransport("http://unreachable.invalid"))
	defer client.Close()

	results := client.CallBatch(context.Background(), nil)
	if results == nil {
		t.Fatal("expected a non-nil empty slice")
	}
	if len(results) != 0 {
		t.Errorf("len(results) = %d, want 0", len(results))
	}
}

// TestCallBatchOrderPreservedUnderConcurrentServerDispatch sends calls whose
// server-side handling time is inversely related to their position, so if
// CallBatch naively returned results in server-completion order the test would
// fail; it must always return them in call order.
func TestCallBatchOrderPreservedUnderConcurrentServerDispatch(t *testing.T) {
	s := jsonrpc.NewServer()
	s.Handle("delay", jsonrpc.Typed(func(ctx context.Context, p struct{ N, DelayMS int }) (any, error) {
		time.Sleep(time.Duration(p.DelayMS) * time.Millisecond)
		return p.N, nil
	}))
	client, cleanup := pipeClientServer(t, s)
	defer cleanup()

	// Later calls finish first (shorter delay), forcing out-of-order arrival.
	calls := []jsonrpc.BatchCall{
		{Method: "delay", Params: map[string]int{"N": 0, "DelayMS": 60}},
		{Method: "delay", Params: map[string]int{"N": 1, "DelayMS": 40}},
		{Method: "delay", Params: map[string]int{"N": 2, "DelayMS": 20}},
		{Method: "delay", Params: map[string]int{"N": 3, "DelayMS": 0}},
	}
	results := client.CallBatch(context.Background(), calls)
	for i, r := range results {
		if r.Err != nil {
			t.Fatalf("results[%d].Err = %v", i, r.Err)
		}
		var got int
		if err := json.Unmarshal(r.Result, &got); err != nil {
			t.Fatalf("decode results[%d]: %v", i, err)
		}
		if got != i {
			t.Errorf("results[%d] = %d, want %d (order not preserved)", i, got, i)
		}
	}
}

// TestCallBatchFallbackTransport exercises the concurrent fallback path used
// for transports without native batch support.
func TestCallBatchFallbackTransport(t *testing.T) {
	srv := httptest.NewServer(batchTestServer())
	defer srv.Close()

	// nonBatchTransport wraps HTTPTransport but hides the CallBatch method so
	// Client.CallBatch must use its per-call fallback.
	client := jsonrpc.NewClient(&nonBatchTransport{inner: jsonrpc.NewHTTPTransport(srv.URL)})
	defer client.Close()

	var sum int
	results := client.CallBatch(context.Background(), []jsonrpc.BatchCall{
		{Method: "add", Params: map[string]int{"A": 5, "B": 6}, Out: &sum},
		{Method: "fail"},
	})
	if len(results) != 2 {
		t.Fatalf("len(results) = %d, want 2", len(results))
	}
	if results[0].Err != nil {
		t.Errorf("results[0].Err = %v", results[0].Err)
	}
	if sum != 11 {
		t.Errorf("sum = %d, want 11", sum)
	}
	var rpcErr *jsonrpc.Error
	if !errors.As(results[1].Err, &rpcErr) || rpcErr.Code != -32001 {
		t.Errorf("results[1].Err = %v, want *Error code -32001", results[1].Err)
	}
}

// nonBatchTransport adapts a Transport but deliberately does not implement
// CallBatch, forcing Client.CallBatch onto its fallback path.
type nonBatchTransport struct {
	inner jsonrpc.Transport
}

func (t *nonBatchTransport) Call(ctx context.Context, req *jsonrpc.Request) (*jsonrpc.Response, error) {
	return t.inner.Call(ctx, req)
}
func (t *nonBatchTransport) Notify(ctx context.Context, req *jsonrpc.Request) error {
	return t.inner.Notify(ctx, req)
}
func (t *nonBatchTransport) Close() error { return t.inner.Close() }
