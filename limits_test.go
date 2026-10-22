package jsonrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// concurrencyProbe is a handler that records how many copies of itself run
// at once.
type concurrencyProbe struct {
	running, peak, calls atomic.Int64
	hold                 time.Duration
}

func (c *concurrencyProbe) handler(ctx context.Context, params json.RawMessage) (any, error) {
	n := c.running.Add(1)
	for {
		p := c.peak.Load()
		if n <= p || c.peak.CompareAndSwap(p, n) {
			break
		}
	}
	time.Sleep(c.hold)
	c.running.Add(-1)
	c.calls.Add(1)
	return "ok", nil
}

func batchOf(n int, method string) []byte {
	var b bytes.Buffer
	b.WriteByte('[')
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"jsonrpc":"2.0","id":%d,"method":"%s"}`, i, method)
	}
	b.WriteByte(']')
	return b.Bytes()
}

// A batch over WithMaxBatch gets one Invalid Request error and runs nothing;
// a batch at the limit runs in full.
func TestMaxBatch(t *testing.T) {
	probe := &concurrencyProbe{}
	s := NewServer(WithMaxBatch(10)).Handle("work", probe.handler)

	out, ok := s.HandleMessage(context.Background(), batchOf(11, "work"))
	if !ok || !strings.Contains(string(out), "batch too large: 11 elements exceeds the limit of 10") || !strings.Contains(string(out), `"code":-32600`) {
		t.Fatalf("over the limit: %s", out)
	}
	if probe.calls.Load() != 0 {
		t.Fatalf("%d handlers ran for a refused batch", probe.calls.Load())
	}

	out, _ = s.HandleMessage(context.Background(), batchOf(10, "work"))
	var resps []Response
	if err := json.Unmarshal(out, &resps); err != nil || len(resps) != 10 || probe.calls.Load() != 10 {
		t.Fatalf("at the limit: %d responses, %d calls, %v", len(resps), probe.calls.Load(), err)
	}
}

// WithMaxConcurrency caps handlers running at once within a batch, and the
// batch starts no more goroutines than the limit.
func TestMaxConcurrencyBatch(t *testing.T) {
	probe := &concurrencyProbe{hold: 20 * time.Millisecond}
	s := NewServer(WithMaxConcurrency(4)).Handle("work", probe.handler)

	base := runtime.NumGoroutine()
	var peakGoroutines atomic.Int64
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			default:
				if n := int64(runtime.NumGoroutine() - base); n > peakGoroutines.Load() {
					peakGoroutines.Store(n)
				}
				time.Sleep(time.Millisecond)
			}
		}
	}()
	out, _ := s.HandleMessage(context.Background(), batchOf(200, "work"))
	close(done)

	var resps []Response
	if err := json.Unmarshal(out, &resps); err != nil || len(resps) != 200 {
		t.Fatalf("%d responses, %v", len(resps), err)
	}
	if p := probe.peak.Load(); p > 4 || p < 2 {
		t.Fatalf("peak concurrent handlers = %d, want 2..4", p)
	}
	if g := peakGoroutines.Load(); g > 20 {
		t.Fatalf("a 200-element batch started %d goroutines under a limit of 4", g)
	}
}

// The limit is shared across the server: many concurrent HTTP requests, each
// a batch, still run no more than the limit at once.
func TestMaxConcurrencyAcrossRequests(t *testing.T) {
	probe := &concurrencyProbe{hold: 10 * time.Millisecond}
	s := NewServer(WithMaxConcurrency(3), WithMaxBatch(50)).Handle("work", probe.handler)
	srv := httptest.NewServer(s)
	defer srv.Close()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			body := []byte(`{"jsonrpc":"2.0","id":1,"method":"work"}`)
			if i%2 == 0 {
				body = batchOf(10, "work")
			}
			resp, err := http.Post(srv.URL, "application/json", bytes.NewReader(body))
			if err != nil {
				t.Error(err)
				return
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}(i)
	}
	wg.Wait()
	if p := probe.peak.Load(); p > 3 {
		t.Fatalf("peak concurrent handlers across requests = %d, want <= 3", p)
	}
	if c := probe.calls.Load(); c != 10*10+10 {
		t.Fatalf("calls = %d, want 110", c)
	}
}

// Stream messages are held to the limit too, and all are answered.
func TestMaxConcurrencyStream(t *testing.T) {
	probe := &concurrencyProbe{hold: 5 * time.Millisecond}
	s := NewServer(WithMaxConcurrency(2)).Handle("work", probe.handler)
	var in bytes.Buffer
	for i := 0; i < 50; i++ {
		fmt.Fprintf(&in, `{"jsonrpc":"2.0","id":%d,"method":"work"}`+"\n", i)
	}
	var out bytes.Buffer
	var mu sync.Mutex
	w := writerFunc(func(p []byte) (int, error) { mu.Lock(); defer mu.Unlock(); return out.Write(p) })
	if err := s.ServeStream(context.Background(), &in, w); err != nil {
		t.Fatal(err)
	}
	if lines := strings.Count(out.String(), "\n"); lines != 50 {
		t.Fatalf("%d responses, want 50", lines)
	}
	if p := probe.peak.Load(); p > 2 {
		t.Fatalf("peak concurrent handlers = %d, want <= 2", p)
	}
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

// A call whose context ends while it waits for a slot fails rather than
// waiting forever.
func TestMaxConcurrencyWaitHonoursContext(t *testing.T) {
	release := make(chan struct{})
	s := NewServer(WithMaxConcurrency(1)).Handle("block", func(ctx context.Context, _ json.RawMessage) (any, error) {
		<-release
		return "done", nil
	})
	go s.HandleMessage(context.Background(), []byte(`{"jsonrpc":"2.0","id":1,"method":"block"}`))
	time.Sleep(20 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	out, _ := s.HandleMessage(ctx, []byte(`{"jsonrpc":"2.0","id":2,"method":"block"}`))
	close(release)
	if !strings.Contains(string(out), "server busy") {
		t.Fatalf("got %s", out)
	}
}

// With no options a batch still runs concurrently, as before.
func TestDefaultsUnlimited(t *testing.T) {
	probe := &concurrencyProbe{hold: 20 * time.Millisecond}
	s := NewServer().Handle("work", probe.handler)
	out, _ := s.HandleMessage(context.Background(), batchOf(2000, "work"))
	var resps []Response
	if err := json.Unmarshal(out, &resps); err != nil || len(resps) != 2000 {
		t.Fatalf("%d responses, %v", len(resps), err)
	}
	if probe.peak.Load() < 100 {
		t.Fatalf("default dispatch should be unbounded, peak %d", probe.peak.Load())
	}
}
