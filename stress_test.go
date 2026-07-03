package jsonrpc_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/paularlott/jsonrpc"
)

// TestStressHTTPConcurrentClients hammers a server with many concurrent HTTP
// clients issuing mixed calls, notifications and batches, checking for races
// and correctness of results.
func TestStressHTTPConcurrentClients(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in -short mode")
	}
	s := jsonrpc.NewServer()
	s.Handle("square", func(ctx context.Context, params json.RawMessage) (any, error) {
		var n int
		if err := jsonrpc.Unmarshal(params, &n); err != nil {
			return nil, err
		}
		return n * n, nil
	})
	s.Handle("noop", func(ctx context.Context, params json.RawMessage) (any, error) {
		return nil, nil
	})
	srv := httptest.NewServer(s)
	defer srv.Close()

	const workers = 32
	const perWorker = 40
	var wg sync.WaitGroup
	errs := make(chan error, workers*perWorker)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := jsonrpc.NewClient(jsonrpc.NewHTTPTransport(srv.URL))
			for i := 0; i < perWorker; i++ {
				var got int
				if err := client.Call(context.Background(), "square", i, &got); err != nil {
					errs <- err
					continue
				}
				if got != i*i {
					errs <- fmt.Errorf("square(%d) = %d", i, got)
				}
				if err := client.Notify(context.Background(), "noop", nil); err != nil {
					errs <- err
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// TestStressLargeBatch sends a big batch and verifies every request gets a
// correct response and notifications are dropped.
func TestStressLargeBatch(t *testing.T) {
	s := jsonrpc.NewServer()
	s.Handle("id", func(ctx context.Context, params json.RawMessage) (any, error) {
		var n int
		_ = jsonrpc.Unmarshal(params, &n)
		return n, nil
	})

	var b strings.Builder
	b.WriteByte('[')
	const n = 500
	expected := 0
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		if i%3 == 0 {
			// notification (no id) -> no response
			fmt.Fprintf(&b, `{"jsonrpc":"2.0","method":"id","params":%d}`, i)
		} else {
			fmt.Fprintf(&b, `{"jsonrpc":"2.0","method":"id","params":%d,"id":%d}`, i, i)
			expected++
		}
	}
	b.WriteByte(']')

	out, has := s.HandleMessage(context.Background(), []byte(b.String()))
	if !has {
		t.Fatal("expected responses")
	}
	var arr []map[string]any
	if err := json.Unmarshal(out, &arr); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(arr) != expected {
		t.Fatalf("got %d responses, want %d", len(arr), expected)
	}
	// Every response's result must equal its id.
	for _, r := range arr {
		if num(r["result"]) != num(r["id"]) {
			t.Errorf("result %v != id %v", r["result"], r["id"])
		}
	}
}

func TestHTTPTransportNon2xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	client := jsonrpc.NewClient(jsonrpc.NewHTTPTransport(srv.URL))
	err := client.Call(context.Background(), "x", nil, nil)
	if err == nil || !strings.Contains(err.Error(), "500") {
		t.Fatalf("expected 500 error, got %v", err)
	}
}
