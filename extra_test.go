package jsonrpc_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/paularlott/jsonrpc"
)

func TestWithHTTPClientOption(t *testing.T) {
	s := jsonrpc.NewServer()
	s.Handle("ping", func(ctx context.Context, params json.RawMessage) (any, error) {
		return "pong", nil
	})
	srv := httptest.NewServer(s)
	defer srv.Close()

	custom := &http.Client{Timeout: 5 * time.Second}
	client := jsonrpc.NewClient(jsonrpc.NewHTTPTransport(srv.URL, jsonrpc.WithHTTPClient(custom)))
	var out string
	if err := client.Call(context.Background(), "ping", nil, &out); err != nil {
		t.Fatal(err)
	}
	if out != "pong" {
		t.Errorf("out = %q", out)
	}
}

func TestProcessTransportWithDir(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	transport, err := jsonrpc.NewProcessTransport(
		os.Args[0],
		nil,
		jsonrpc.WithEnv(append(os.Environ(), childEnvVar+"=1")),
		jsonrpc.WithStderr(io.Discard),
		jsonrpc.WithDir(wd),
	)
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}
	client := jsonrpc.NewClient(transport)
	defer client.Close()

	var sum int
	if err := client.Call(context.Background(), "add", []int{2, 3}, &sum); err != nil {
		t.Fatal(err)
	}
	if sum != 5 {
		t.Errorf("sum = %d", sum)
	}
}

func TestStreamTransportNotifyAfterClose(t *testing.T) {
	clientReader, serverWriter := io.Pipe()
	serverReader, clientWriter := io.Pipe()
	transport := jsonrpc.NewStreamTransport(clientReader, clientWriter)

	transport.Close()
	serverWriter.Close()
	serverReader.Close()

	client := jsonrpc.NewClient(transport)
	if err := client.Notify(context.Background(), "x", nil); err == nil {
		t.Fatal("expected error notifying on closed transport")
	}
}

func TestClientNilOutDiscardsResult(t *testing.T) {
	s := jsonrpc.NewServer()
	s.Handle("val", func(ctx context.Context, params json.RawMessage) (any, error) {
		return map[string]int{"x": 1}, nil
	})
	srv := httptest.NewServer(s)
	defer srv.Close()

	client := jsonrpc.NewClient(jsonrpc.NewHTTPTransport(srv.URL))
	// out == nil: result is fetched successfully but discarded.
	if err := client.Call(context.Background(), "val", nil, nil); err != nil {
		t.Fatal(err)
	}
}

func TestServeStreamWritesEveryResponseOnce(t *testing.T) {
	// Regression guard: interleaved concurrent writes must not corrupt or drop
	// lines. Feed many single requests and count responses.
	s := jsonrpc.NewServer()
	s.Handle("id", func(ctx context.Context, params json.RawMessage) (any, error) {
		return params, nil
	})

	var in []byte
	const n = 200
	for i := 0; i < n; i++ {
		line := []byte(`{"jsonrpc":"2.0","method":"id","params":` + itoa(i) + `,"id":` + itoa(i) + "}\n")
		in = append(in, line...)
	}

	pr, pw := io.Pipe()
	go func() {
		pw.Write(in)
		pw.Close()
	}()

	var out safeBuffer
	if err := s.ServeStream(context.Background(), pr, &out); err != nil {
		t.Fatal(err)
	}
	lines := readLines(t, out.Bytes())
	if len(lines) != n {
		t.Fatalf("got %d responses, want %d", len(lines), n)
	}
	seen := map[float64]bool{}
	for _, l := range lines {
		seen[num(l["id"])] = true
	}
	if len(seen) != n {
		t.Errorf("expected %d distinct ids, got %d", n, len(seen))
	}
}

type safeBuffer struct {
	mu  sync.Mutex
	buf []byte
}

func (b *safeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf = append(b.buf, p...)
	return len(p), nil
}

func (b *safeBuffer) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]byte, len(b.buf))
	copy(out, b.buf)
	return out
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}
