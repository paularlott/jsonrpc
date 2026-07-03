package jsonrpc_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/paularlott/jsonrpc"
)

// newPeerPair wires two Peers back-to-back over two in-process pipes, so each
// side can both call and serve the other — the bidirectional shape the Peer
// exists for. The returned cleanup closes both peers and all four pipe ends.
func newPeerPair(t *testing.T, serverA, serverB *jsonrpc.Server) (*jsonrpc.Peer, *jsonrpc.Peer, func()) {
	t.Helper()
	aReader, bWriter := io.Pipe() // A reads what B writes
	bReader, aWriter := io.Pipe() // B reads what A writes
	a := jsonrpc.NewPeer(aReader, aWriter, serverA)
	b := jsonrpc.NewPeer(bReader, bWriter, serverB)
	go func() { _ = a.Serve() }()
	go func() { _ = b.Serve() }()
	cleanup := func() {
		_ = a.Close()
		_ = b.Close()
		_ = aReader.Close()
		_ = aWriter.Close()
		_ = bReader.Close()
		_ = bWriter.Close()
	}
	return a, b, cleanup
}

// TestPeerBidirectionalCall exercises the core reason Peer exists: while B is
// handling a call from A, B calls back into A (a host callback). This must not
// deadlock — B's reader must stay free to receive A's callback response while
// A is still waiting for B's original response.
func TestPeerBidirectionalCall(t *testing.T) {
	serverA := jsonrpc.NewServer()
	serverB := jsonrpc.NewServer()
	a, b, cleanup := newPeerPair(t, serverA, serverB)
	defer cleanup()

	serverA.Handle("host.method", func(ctx context.Context, params json.RawMessage) (any, error) {
		return "from-host", nil
	})

	var bSawCallback string
	serverB.Handle("plugin.method", func(ctx context.Context, params json.RawMessage) (any, error) {
		// Reverse-direction call back to A while A is waiting on this very call.
		var cb string
		if err := b.Client().Call(ctx, "host.method", nil, &cb); err != nil {
			return nil, err
		}
		bSawCallback = cb
		return "from-plugin", nil
	})

	var result string
	if err := a.Client().Call(context.Background(), "plugin.method", nil, &result); err != nil {
		t.Fatalf("A Call: %v", err)
	}
	if result != "from-plugin" {
		t.Errorf("result = %q, want %q", result, "from-plugin")
	}
	if bSawCallback != "from-host" {
		t.Errorf("B callback result = %q, want %q", bSawCallback, "from-host")
	}
}

// TestPeerNotificationsBothDirections verifies notifications carry no id and
// produce no response, yet still reach the handler on each side.
func TestPeerNotificationsBothDirections(t *testing.T) {
	serverA := jsonrpc.NewServer()
	serverB := jsonrpc.NewServer()
	a, b, cleanup := newPeerPair(t, serverA, serverB)
	defer cleanup()

	gotA := make(chan string, 1)
	gotB := make(chan string, 1)
	serverA.Handle("to-a", func(ctx context.Context, params json.RawMessage) (any, error) {
		gotA <- "a"
		return nil, nil
	})
	serverB.Handle("to-b", func(ctx context.Context, params json.RawMessage) (any, error) {
		gotB <- "b"
		return nil, nil
	})

	if err := a.Client().Notify(context.Background(), "to-b", nil); err != nil {
		t.Fatalf("A Notify: %v", err)
	}
	if err := b.Client().Notify(context.Background(), "to-a", nil); err != nil {
		t.Fatalf("B Notify: %v", err)
	}

	for _, ch := range []chan string{gotB, gotA} {
		select {
		case <-ch:
		case <-time.After(2 * time.Second):
			t.Fatal("notification handler did not fire")
		}
	}
}

// TestPeerBatchFromClient verifies a Client.CallBatch over a Peer sends a single
// batch frame and correlates the responses by id.
func TestPeerBatchFromClient(t *testing.T) {
	serverB := jsonrpc.NewServer()
	serverB.Handle("echo", func(ctx context.Context, params json.RawMessage) (any, error) {
		var v int
		_ = jsonrpc.Unmarshal(params, &v)
		return v * 2, nil
	})
	a, _, cleanup := newPeerPair(t, jsonrpc.NewServer(), serverB)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	out := make([]int, 4)
	results := a.Client().CallBatch(ctx, []jsonrpc.BatchCall{
		{Method: "echo", Params: 1, Out: &out[0]},
		{Method: "echo", Params: 2, Out: &out[1]},
		{Method: "echo", Params: 3, Out: &out[2]},
		{Method: "echo", Params: 4, Out: &out[3]},
	})
	for i, r := range results {
		if r.Err != nil {
			t.Fatalf("batch[%d]: %v", i, r.Err)
		}
	}
	want := []int{2, 4, 6, 8}
	for i := range want {
		if out[i] != want[i] {
			t.Errorf("out[%d] = %d, want %d", i, out[i], want[i])
		}
	}
}

// TestPeerInboundBatch verifies a batch of inbound requests is dispatched and
// answered.
func TestPeerInboundBatch(t *testing.T) {
	serverB := jsonrpc.NewServer()
	serverB.Handle("inc", func(ctx context.Context, params json.RawMessage) (any, error) {
		var v int
		_ = jsonrpc.Unmarshal(params, &v)
		return v + 1, nil
	})
	a, _, cleanup := newPeerPair(t, jsonrpc.NewServer(), serverB)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	out := make([]int, 3)
	results := a.Client().CallBatch(ctx, []jsonrpc.BatchCall{
		{Method: "inc", Params: 10, Out: &out[0]},
		{Method: "inc", Params: 20, Out: &out[1]},
		{Method: "inc", Params: 30, Out: &out[2]},
	})
	for i, r := range results {
		if r.Err != nil {
			t.Fatalf("batch[%d]: %v", i, r.Err)
		}
	}
	want := []int{11, 21, 31}
	for i := range want {
		if out[i] != want[i] {
			t.Errorf("out[%d] = %d, want %d", i, out[i], want[i])
		}
	}
}

// TestPeerVoidResponse exercises the classification fix: a handler that returns
// (nil, nil) produces a response with no result and no error member. Such a
// "void" response (just an id) must still be delivered to the pending call and
// not be mistaken for an inbound request. (scriptling's plugin.shutdown and
// object.destroy rely on this.)
func TestPeerVoidResponse(t *testing.T) {
	serverB := jsonrpc.NewServer()
	serverB.Handle("noop", func(ctx context.Context, params json.RawMessage) (any, error) {
		return nil, nil // void result
	})
	a, _, cleanup := newPeerPair(t, jsonrpc.NewServer(), serverB)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := a.Client().Call(ctx, "noop", nil, nil); err != nil {
		t.Fatalf("noop call: %v", err)
	}
}

// TestPeerMethodNotFound verifies an inbound request for an unregistered method
// yields a JSON-RPC method-not-found error to the caller.
func TestPeerMethodNotFound(t *testing.T) {
	a, _, cleanup := newPeerPair(t, jsonrpc.NewServer(), jsonrpc.NewServer())
	defer cleanup()

	err := a.Client().Call(context.Background(), "nope", nil, nil)
	var rpcErr *jsonrpc.Error
	if !errors.As(err, &rpcErr) {
		t.Fatalf("expected *jsonrpc.Error, got %T: %v", err, err)
	}
	if rpcErr.Code != jsonrpc.CodeMethodNotFound {
		t.Errorf("code = %d, want %d", rpcErr.Code, jsonrpc.CodeMethodNotFound)
	}
}

// TestPeerCloseFailsCalls verifies that after Close, outbound calls fail rather
// than hang, and Done is closed.
func TestPeerCloseFailsCalls(t *testing.T) {
	a, _, cleanup := newPeerPair(t, jsonrpc.NewServer(), jsonrpc.NewServer())
	// Note: do not defer cleanup; we close explicitly and then call cleanup to
	// release pipes.

	if err := a.Close(); err != nil {
		t.Errorf("close: %v", err)
	}
	cleanup()

	select {
	case <-a.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("Done not signalled after Close")
	}

	done := make(chan error, 1)
	go func() {
		done <- a.Client().Call(context.Background(), "x", nil, nil)
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Error("expected error after Close")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Call after Close hung")
	}
}

// TestPeerConcurrentCalls exercises many simultaneous bidirectional calls,
// guarding against races and id-correlation mixups under -race.
func TestPeerConcurrentCalls(t *testing.T) {
	serverB := jsonrpc.NewServer()
	serverB.Handle("square", func(ctx context.Context, params json.RawMessage) (any, error) {
		var v int
		_ = jsonrpc.Unmarshal(params, &v)
		return v * v, nil
	})
	a, _, cleanup := newPeerPair(t, jsonrpc.NewServer(), serverB)
	defer cleanup()

	const N = 50
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var got int
			if err := a.Client().Call(context.Background(), "square", i, &got); err != nil {
				t.Errorf("call %d: %v", i, err)
				return
			}
			if got != i*i {
				t.Errorf("call %d: got %d, want %d", i, got, i*i)
			}
		}(i)
	}
	wg.Wait()
}
