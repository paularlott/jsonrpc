package jsonrpc_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/paularlott/jsonrpc"
)

// readLines decodes NDJSON output into per-line objects.
func readLines(t *testing.T, out []byte) []map[string]any {
	t.Helper()
	var res []map[string]any
	dec := json.NewDecoder(bytes.NewReader(out))
	dec.UseNumber()
	for {
		var m map[string]any
		if err := dec.Decode(&m); err != nil {
			break
		}
		res = append(res, m)
	}
	return res
}

func TestServeStreamSingle(t *testing.T) {
	s := echoServer(t)
	in := `{"jsonrpc":"2.0","method":"echo","params":{"a":1},"id":1}`
	var out bytes.Buffer
	if err := s.ServeStream(context.Background(), strings.NewReader(in), &out); err != nil {
		t.Fatalf("ServeStream: %v", err)
	}
	lines := readLines(t, out.Bytes())
	if len(lines) != 1 {
		t.Fatalf("expected 1 line, got %d", len(lines))
	}
	if num(lines[0]["id"]) != 1 {
		t.Errorf("id = %v", lines[0]["id"])
	}
}

func TestServeStreamMultipleMessages(t *testing.T) {
	s := echoServer(t)
	// Two whitespace-separated JSON values (no explicit newline needed).
	in := `{"jsonrpc":"2.0","method":"echo","params":1,"id":1} {"jsonrpc":"2.0","method":"echo","params":2,"id":2}`
	var out bytes.Buffer
	if err := s.ServeStream(context.Background(), strings.NewReader(in), &out); err != nil {
		t.Fatalf("ServeStream: %v", err)
	}
	lines := readLines(t, out.Bytes())
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines, got %d: %s", len(lines), out.String())
	}
}

func TestServeStreamNotificationNoOutput(t *testing.T) {
	s := echoServer(t)
	in := `{"jsonrpc":"2.0","method":"echo","params":1}`
	var out bytes.Buffer
	if err := s.ServeStream(context.Background(), strings.NewReader(in), &out); err != nil {
		t.Fatalf("ServeStream: %v", err)
	}
	if out.Len() != 0 {
		t.Errorf("expected no output, got %q", out.String())
	}
}

func TestServeStreamBatchIsSingleLine(t *testing.T) {
	s := subtractServer(t)
	in := `[{"jsonrpc":"2.0","method":"subtract","params":[9,4],"id":1},{"jsonrpc":"2.0","method":"subtract","params":[3,1],"id":2}]`
	var out bytes.Buffer
	if err := s.ServeStream(context.Background(), strings.NewReader(in), &out); err != nil {
		t.Fatalf("ServeStream: %v", err)
	}
	// The whole batch response must be one JSON array on one line.
	trimmed := bytes.TrimSpace(out.Bytes())
	if trimmed[0] != '[' {
		t.Fatalf("expected array output, got %s", trimmed)
	}
	var arr []map[string]any
	if err := json.Unmarshal(trimmed, &arr); err != nil {
		t.Fatalf("output not a single array: %v (%s)", err, trimmed)
	}
	if len(arr) != 2 {
		t.Errorf("expected 2 responses, got %d", len(arr))
	}
}

func TestServeStreamParseErrorMidStream(t *testing.T) {
	s := echoServer(t)
	in := `{not valid json`
	var out bytes.Buffer
	err := s.ServeStream(context.Background(), strings.NewReader(in), &out)
	if err == nil {
		t.Fatal("expected a fatal decode error")
	}
	lines := readLines(t, out.Bytes())
	if len(lines) != 1 {
		t.Fatalf("expected 1 parse-error line, got %d", len(lines))
	}
	e := lines[0]["error"].(map[string]any)
	if num(e["code"]) != jsonrpc.CodeParseError {
		t.Errorf("code = %v, want parse error", e["code"])
	}
}

func TestServeStreamNumberPrecision(t *testing.T) {
	// The library hands params to the handler as raw bytes and marshals a
	// returned json.RawMessage verbatim, so a handler that echoes the raw params
	// preserves 2^53+1 exactly (it cannot survive a round-trip through float64).
	s := jsonrpc.NewServer()
	s.Handle("echo", func(ctx context.Context, params json.RawMessage) (any, error) {
		return params, nil
	})
	in := `{"jsonrpc":"2.0","method":"echo","params":{"big":9007199254740993},"id":1}`
	var out bytes.Buffer
	if err := s.ServeStream(context.Background(), strings.NewReader(in), &out); err != nil {
		t.Fatal(err)
	}
	lines := readLines(t, out.Bytes())
	res := lines[0]["result"].(map[string]any)
	n, ok := res["big"].(json.Number)
	if !ok {
		t.Fatalf("big = %T, want json.Number", res["big"])
	}
	if n.String() != "9007199254740993" {
		t.Errorf("big = %s, want 9007199254740993", n.String())
	}
}

// TestServeStreamConcurrent verifies handlers run concurrently: two ~120ms
// handlers should complete in well under their serial sum.
func TestServeStreamConcurrent(t *testing.T) {
	s := jsonrpc.NewServer()
	slow := func(ctx context.Context, params json.RawMessage) (any, error) {
		time.Sleep(120 * time.Millisecond)
		return "ok", nil
	}
	s.Handle("a", slow)
	s.Handle("b", slow)

	in := "{\"jsonrpc\":\"2.0\",\"method\":\"a\",\"id\":1}\n{\"jsonrpc\":\"2.0\",\"method\":\"b\",\"id\":2}\n"
	var out bytes.Buffer
	var mu sync.Mutex
	start := time.Now()
	if err := s.ServeStream(context.Background(), strings.NewReader(in), &syncWriter{w: &out, mu: &mu}); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	if elapsed > 220*time.Millisecond {
		t.Errorf("expected concurrent execution < 220ms, took %v", elapsed)
	}
	lines := readLines(t, out.Bytes())
	if len(lines) != 2 {
		t.Fatalf("expected 2 responses, got %d", len(lines))
	}
}

// syncWriter guards a buffer so the concurrency test's writer is race-free even
// though ServeStream already serialises writes (belt and braces for -race).
type syncWriter struct {
	w  io.Writer
	mu *sync.Mutex
}

func (s *syncWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Write(p)
}

// pipeClientServer wires a Client's StreamTransport to a Server's ServeStream
// over two in-memory pipes, exercising the full stdio round-trip in-process.
func pipeClientServer(t *testing.T, s *jsonrpc.Server) (*jsonrpc.Client, func()) {
	t.Helper()
	clientReader, serverWriter := io.Pipe() // server -> client
	serverReader, clientWriter := io.Pipe() // client -> server

	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		_ = s.ServeStream(context.Background(), serverReader, serverWriter)
	}()

	transport := jsonrpc.NewStreamTransport(clientReader, clientWriter)
	client := jsonrpc.NewClient(transport)

	cleanup := func() {
		client.Close()
		clientWriter.Close() // EOF to server -> ServeStream returns
		<-serveDone
		serverWriter.Close()
	}
	return client, cleanup
}

func TestClientOverStreamTransport(t *testing.T) {
	s := jsonrpc.NewServer()
	s.Handle("upper", func(ctx context.Context, params json.RawMessage) (any, error) {
		var str string
		if err := jsonrpc.Unmarshal(params, &str); err != nil {
			return nil, err
		}
		return strings.ToUpper(str), nil
	})
	client, cleanup := pipeClientServer(t, s)
	defer cleanup()

	var out string
	if err := client.Call(context.Background(), "upper", "hello", &out); err != nil {
		t.Fatalf("call: %v", err)
	}
	if out != "HELLO" {
		t.Errorf("out = %q, want HELLO", out)
	}
}

func TestClientOverStreamConcurrent(t *testing.T) {
	s := jsonrpc.NewServer()
	s.Handle("double", func(ctx context.Context, params json.RawMessage) (any, error) {
		var n int
		if err := jsonrpc.Unmarshal(params, &n); err != nil {
			return nil, err
		}
		time.Sleep(10 * time.Millisecond)
		return n * 2, nil
	})
	client, cleanup := pipeClientServer(t, s)
	defer cleanup()

	const N = 50
	var wg sync.WaitGroup
	errs := make(chan error, N)
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var got int
			if err := client.Call(context.Background(), "double", i, &got); err != nil {
				errs <- err
				return
			}
			if got != i*2 {
				errs <- fmt.Errorf("double(%d) = %d", i, got)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestClientOverStreamNotification(t *testing.T) {
	var mu sync.Mutex
	got := make(chan string, 1)
	s := jsonrpc.NewServer()
	s.Handle("log", func(ctx context.Context, params json.RawMessage) (any, error) {
		var msg string
		jsonrpc.Unmarshal(params, &msg)
		mu.Lock()
		select {
		case got <- msg:
		default:
		}
		mu.Unlock()
		return nil, nil
	})
	client, cleanup := pipeClientServer(t, s)
	defer cleanup()

	if err := client.Notify(context.Background(), "log", "hi"); err != nil {
		t.Fatalf("notify: %v", err)
	}
	select {
	case msg := <-got:
		if msg != "hi" {
			t.Errorf("got %q", msg)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("notification handler never ran")
	}
}

func TestStreamTransportContextCancel(t *testing.T) {
	s := jsonrpc.NewServer()
	release := make(chan struct{})
	s.Handle("block", func(ctx context.Context, params json.RawMessage) (any, error) {
		<-release
		return "done", nil
	})
	client, cleanup := pipeClientServer(t, s)
	defer cleanup()
	defer close(release)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := client.Call(ctx, "block", nil, nil)
	if err == nil {
		t.Fatal("expected context deadline error")
	}
	if !strings.Contains(err.Error(), "context") {
		t.Errorf("err = %v, want context error", err)
	}
}

func TestStreamTransportClosed(t *testing.T) {
	clientReader, serverWriter := io.Pipe()
	serverReader, clientWriter := io.Pipe()
	transport := jsonrpc.NewStreamTransport(clientReader, clientWriter)
	client := jsonrpc.NewClient(transport)

	// Close the server side so the transport's read loop sees EOF.
	serverWriter.Close()
	serverReader.Close()
	transport.Close()

	err := client.Call(context.Background(), "whatever", nil, nil)
	if err == nil {
		t.Fatal("expected error calling on closed transport")
	}
}
