package jsonrpc_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/paularlott/jsonrpc"
)

// childEnvVar switches the test binary into "child server" mode: instead of
// running tests it serves JSON-RPC over stdio. This lets the ProcessTransport
// test spawn a real subprocess without a separate helper binary.
const childEnvVar = "JSONRPC_TEST_CHILD"

// childPeerEnv switches the test binary into "child peer" mode: a bidirectional
// JSON-RPC peer over stdio that both serves "reverse" and calls "parent.echo"
// back, exercising NewProcessPeer over a real subprocess.
const childPeerEnv = "JSONRPC_TEST_CHILD_PEER"

func TestMain(m *testing.M) {
	if os.Getenv(childPeerEnv) == "1" {
		runChildPeer()
		return
	}
	if os.Getenv(childEnvVar) == "1" {
		runChildServer()
		return
	}
	os.Exit(m.Run())
}

// runChildPeer acts as a bidirectional JSON-RPC peer: it serves "reverse" by
// calling "parent.echo" back on its caller, proving NewProcessPeer can do
// reverse-direction calls over real stdio.
func runChildPeer() {
	srv := jsonrpc.NewServer()
	peer := jsonrpc.NewPeer(os.Stdin, os.Stdout, srv)
	srv.Handle("reverse", func(ctx context.Context, params json.RawMessage) (any, error) {
		var s string
		if err := jsonrpc.Unmarshal(params, &s); err != nil {
			return nil, err
		}
		var echoed string
		if err := peer.Client().Call(ctx, "parent.echo", s, &echoed); err != nil {
			return nil, err
		}
		return echoed, nil
	})
	_ = peer.Serve()
}

func runChildServer() {
	s := jsonrpc.NewServer()
	s.Handle("echo", func(ctx context.Context, params json.RawMessage) (any, error) {
		var v any
		_ = jsonrpc.Unmarshal(params, &v)
		return v, nil
	})
	s.Handle("add", func(ctx context.Context, params json.RawMessage) (any, error) {
		var nums []int
		if err := jsonrpc.Unmarshal(params, &nums); err != nil {
			return nil, err
		}
		sum := 0
		for _, n := range nums {
			sum += n
		}
		return sum, nil
	})
	// "exit" lets a test ask the child to terminate itself (after replying, so
	// the caller observes a clean response before the stream closes).
	s.Handle("exit", func(ctx context.Context, params json.RawMessage) (any, error) {
		go os.Exit(0)
		return nil, nil
	})
	_ = s.ServeStream(context.Background(), os.Stdin, os.Stdout)
}

func newChildTransport(t *testing.T) *jsonrpc.StreamTransport {
	t.Helper()
	transport, err := jsonrpc.NewProcessTransport(
		os.Args[0],
		nil,
		jsonrpc.WithEnv(append(os.Environ(), childEnvVar+"=1")),
		jsonrpc.WithStderr(io.Discard),
		jsonrpc.WithShutdownTimeout(2*time.Second),
	)
	if err != nil {
		t.Fatalf("spawn child: %v", err)
	}
	return transport
}

func TestProcessTransportRoundTrip(t *testing.T) {
	transport := newChildTransport(t)
	client := jsonrpc.NewClient(transport)
	defer client.Close()

	var sum int
	if err := client.Call(context.Background(), "add", []int{1, 2, 3, 4}, &sum); err != nil {
		t.Fatalf("call add: %v", err)
	}
	if sum != 10 {
		t.Errorf("sum = %d, want 10", sum)
	}

	var echoed string
	if err := client.Call(context.Background(), "echo", "roundtrip", &echoed); err != nil {
		t.Fatalf("call echo: %v", err)
	}
	if echoed != "roundtrip" {
		t.Errorf("echo = %q", echoed)
	}
}

func TestProcessTransportMethodNotFound(t *testing.T) {
	transport := newChildTransport(t)
	client := jsonrpc.NewClient(transport)
	defer client.Close()

	err := client.Call(context.Background(), "does_not_exist", nil, nil)
	var rpcErr *jsonrpc.Error
	if !asErr(err, &rpcErr) {
		t.Fatalf("expected *jsonrpc.Error, got %T: %v", err, err)
	}
	if rpcErr.Code != jsonrpc.CodeMethodNotFound {
		t.Errorf("code = %d, want %d", rpcErr.Code, jsonrpc.CodeMethodNotFound)
	}
}

func TestProcessTransportCloseIsClean(t *testing.T) {
	transport := newChildTransport(t)
	client := jsonrpc.NewClient(transport)

	var out int
	if err := client.Call(context.Background(), "add", []int{5, 5}, &out); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Errorf("close: %v", err)
	}
	// Calls after close must fail rather than hang.
	done := make(chan error, 1)
	go func() {
		done <- client.Call(context.Background(), "add", []int{1}, &out)
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Error("expected error after close")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("call after close hung")
	}
}

// ensure strings import is used (kept for potential message assertions).
var _ = strings.Contains

// TestProcessPeerBidirectional spawns this test binary as a bidirectional
// JSON-RPC peer and drives it through NewProcessPeer: the parent calls the
// child's "reverse" method, which calls "parent.echo" back over the same stdio
// connection before returning. This proves the subprocess Peer supports
// reverse-direction calls (the host-callback shape) over a real pipe.
func TestProcessPeerBidirectional(t *testing.T) {
	parentSrv := jsonrpc.NewServer()
	parentSrv.Handle("parent.echo", func(ctx context.Context, params json.RawMessage) (any, error) {
		var s string
		_ = jsonrpc.Unmarshal(params, &s)
		return "parent:" + s, nil
	})
	peer, err := jsonrpc.NewProcessPeer(
		os.Args[0], nil, parentSrv,
		jsonrpc.WithEnv(append(os.Environ(), childPeerEnv+"=1")),
		jsonrpc.WithStderr(io.Discard),
		jsonrpc.WithShutdownTimeout(2*time.Second),
	)
	if err != nil {
		t.Fatalf("spawn child peer: %v", err)
	}
	defer peer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var result string
	if err := peer.Client().Call(ctx, "reverse", "hi", &result); err != nil {
		t.Fatalf("reverse call: %v", err)
	}
	if result != "parent:hi" {
		t.Errorf("result = %q, want %q", result, "parent:hi")
	}
}

// TestProcessPeerOnExit verifies WithOnExit fires for a subprocess Peer.
func TestProcessPeerOnExit(t *testing.T) {
	exited := make(chan error, 1)
	parentSrv := jsonrpc.NewServer()
	peer, err := jsonrpc.NewProcessPeer(
		os.Args[0], nil, parentSrv,
		jsonrpc.WithEnv(append(os.Environ(), childEnvVar+"=1")),
		jsonrpc.WithStderr(io.Discard),
		jsonrpc.WithShutdownTimeout(2*time.Second),
		jsonrpc.WithOnExit(func(err error) { exited <- err }),
	)
	if err != nil {
		t.Fatalf("spawn child peer: %v", err)
	}
	if err := peer.Close(); err != nil {
		t.Errorf("close: %v", err)
	}
	select {
	case err := <-exited:
		if err != nil {
			t.Errorf("onExit err = %v, want nil", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("onExit did not fire after Close")
	}
}

func TestProcessTransportOnExitClean(t *testing.T) {
	exited := make(chan error, 1)
	transport, err := jsonrpc.NewProcessTransport(
		os.Args[0],
		nil,
		jsonrpc.WithEnv(append(os.Environ(), childEnvVar+"=1")),
		jsonrpc.WithStderr(io.Discard),
		jsonrpc.WithShutdownTimeout(2*time.Second),
		jsonrpc.WithOnExit(func(err error) { exited <- err }),
	)
	if err != nil {
		t.Fatalf("spawn child: %v", err)
	}
	client := jsonrpc.NewClient(transport)

	var sum int
	if err := client.Call(context.Background(), "add", []int{2, 3}, &sum); err != nil {
		t.Fatalf("call add: %v", err)
	}

	// Closing the transport ends the child cleanly; OnExit must fire once.
	if err := client.Close(); err != nil {
		t.Errorf("close: %v", err)
	}
	select {
	case err := <-exited:
		if err != nil {
			t.Errorf("onExit err = %v, want nil for clean exit", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("onExit did not fire after Close")
	}
}

func TestProcessTransportOnExitSpontaneous(t *testing.T) {
	exited := make(chan error, 1)
	transport, err := jsonrpc.NewProcessTransport(
		os.Args[0],
		nil,
		jsonrpc.WithEnv(append(os.Environ(), childEnvVar+"=1")),
		jsonrpc.WithStderr(io.Discard),
		jsonrpc.WithShutdownTimeout(5*time.Second),
		jsonrpc.WithOnExit(func(err error) { exited <- err }),
	)
	if err != nil {
		t.Fatalf("spawn child: %v", err)
	}
	client := jsonrpc.NewClient(transport)
	defer client.Close()

	// Ask the child to terminate itself. The "exit" handler calls os.Exit in a
	// goroutine, so its response races process termination: the call may return
	// nil (response landed first) or ErrClosed (the process exited first). Both
	// are expected — the call is only a trigger; what matters is that the child
	// dies, is reaped (no zombie), and OnExit fires without Close.
	if err := client.Call(context.Background(), "exit", nil, nil); err != nil &&
		!errors.Is(err, jsonrpc.ErrClosed) {
		t.Fatalf("call exit: %v", err)
	}
	select {
	case <-exited:
		// OnExit fired: child reaped without Close.
	case <-time.After(3 * time.Second):
		t.Fatal("onExit did not fire after spontaneous child exit")
	}

	// After the child has gone, further calls must fail rather than hang.
	done := make(chan error, 1)
	go func() {
		var out int
		done <- client.Call(context.Background(), "add", []int{1}, &out)
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Error("expected error after child exit")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("call after child exit hung")
	}
}
