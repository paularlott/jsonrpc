package jsonrpc

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

// ErrClosed is returned by a stream transport when a call cannot complete
// because the transport has been closed or its underlying stream ended.
var ErrClosed = errors.New("jsonrpc: transport closed")

// StreamTransport is a [Transport] over a pair of byte streams carrying
// newline-delimited JSON-RPC messages. It is the client-side counterpart to
// [Server.ServeStream] and backs both in-process pipes and subprocess
// communication (see [NewProcessTransport]).
//
// A background goroutine reads responses from the input stream and correlates
// them to pending calls by request id, so many calls and notifications may be
// in flight concurrently. It is safe for concurrent use.
type StreamTransport struct {
	w       io.Writer
	writeMu sync.Mutex

	mu      sync.Mutex
	pending map[string]chan *Response
	readErr error

	done      chan struct{}
	doneOnce  sync.Once
	closeOnce sync.Once
	closeFn   func() error
}

// StreamOption configures a [StreamTransport].
type StreamOption func(*StreamTransport)

// WithCloseFunc registers a function run once when the transport is closed,
// after the read loop is signalled to stop. [NewProcessTransport] uses it to
// shut down the child process.
func WithCloseFunc(fn func() error) StreamOption {
	return func(t *StreamTransport) { t.closeFn = fn }
}

// NewStreamTransport creates a transport that writes requests to out and reads
// responses from in. It starts the background read loop immediately; call
// [StreamTransport.Close] to stop it and release resources.
func NewStreamTransport(in io.Reader, out io.Writer, opts ...StreamOption) *StreamTransport {
	t := &StreamTransport{
		w:       out,
		pending: make(map[string]chan *Response),
		done:    make(chan struct{}),
	}
	for _, opt := range opts {
		opt(t)
	}
	go t.readLoop(in)
	return t
}

func (t *StreamTransport) readLoop(in io.Reader) {
	decoder := json.NewDecoder(bufio.NewReader(in))
	for {
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			t.fail(err)
			return
		}
		trimmed := bytes.TrimSpace(raw)
		if len(trimmed) == 0 {
			continue
		}
		if trimmed[0] == '[' {
			var responses []*Response
			if err := json.Unmarshal(raw, &responses); err != nil {
				continue
			}
			for _, resp := range responses {
				t.deliver(resp)
			}
			continue
		}
		var resp Response
		if err := json.Unmarshal(raw, &resp); err != nil {
			continue
		}
		t.deliver(&resp)
	}
}

func (t *StreamTransport) deliver(resp *Response) {
	if resp == nil {
		return
	}
	key := string(bytes.TrimSpace(resp.ID))
	t.mu.Lock()
	ch, ok := t.pending[key]
	if ok {
		delete(t.pending, key)
	}
	t.mu.Unlock()
	if ok {
		ch <- resp
	}
}

func (t *StreamTransport) fail(err error) {
	t.mu.Lock()
	if t.readErr == nil {
		t.readErr = err
	}
	t.mu.Unlock()
	t.doneOnce.Do(func() { close(t.done) })
}

func (t *StreamTransport) write(v any) error {
	body, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("jsonrpc: marshal request: %w", err)
	}
	body = append(body, '\n')
	t.writeMu.Lock()
	defer t.writeMu.Unlock()
	_, err = t.w.Write(body)
	return err
}

// Call implements [Transport].
func (t *StreamTransport) Call(ctx context.Context, req *Request) (*Response, error) {
	key := string(bytes.TrimSpace(req.ID))
	ch := make(chan *Response, 1)

	t.mu.Lock()
	select {
	case <-t.done:
		t.mu.Unlock()
		return nil, t.closedError()
	default:
	}
	if _, exists := t.pending[key]; exists {
		t.mu.Unlock()
		return nil, fmt.Errorf("jsonrpc: duplicate in-flight request id %s", key)
	}
	t.pending[key] = ch
	t.mu.Unlock()

	if err := t.write(req); err != nil {
		t.mu.Lock()
		delete(t.pending, key)
		t.mu.Unlock()
		return nil, err
	}

	select {
	case resp := <-ch:
		return resp, nil
	case <-ctx.Done():
		t.mu.Lock()
		delete(t.pending, key)
		t.mu.Unlock()
		return nil, ctx.Err()
	case <-t.done:
		return nil, t.closedError()
	}
}

// CallBatch implements the batch-capable transport extension used by
// [Client.CallBatch]: it writes the whole batch as a single JSON-RPC array
// line and collects each response as it arrives, in any order, via the same
// id-correlated delivery used by Call.
func (t *StreamTransport) CallBatch(ctx context.Context, reqs []*Request) ([]*Response, error) {
	if len(reqs) == 0 {
		return nil, nil
	}

	keys := make([]string, len(reqs))
	chans := make(map[string]chan *Response, len(reqs))

	t.mu.Lock()
	select {
	case <-t.done:
		t.mu.Unlock()
		return nil, t.closedError()
	default:
	}
	for i, req := range reqs {
		key := string(bytes.TrimSpace(req.ID))
		keys[i] = key
		if _, exists := t.pending[key]; exists {
			t.mu.Unlock()
			return nil, fmt.Errorf("jsonrpc: duplicate in-flight request id %s", key)
		}
		ch := make(chan *Response, 1)
		t.pending[key] = ch
		chans[key] = ch
	}
	t.mu.Unlock()

	cleanup := func(remaining map[string]chan *Response) {
		t.mu.Lock()
		for key := range remaining {
			delete(t.pending, key)
		}
		t.mu.Unlock()
	}

	if err := t.write(reqs); err != nil {
		cleanup(chans)
		return nil, err
	}

	resps := make([]*Response, len(reqs))
	remaining := make(map[string]chan *Response, len(chans))
	for k, v := range chans {
		remaining[k] = v
	}

	for i, key := range keys {
		ch := chans[key]
		select {
		case resp := <-ch:
			resps[i] = resp
			delete(remaining, key)
		case <-ctx.Done():
			cleanup(remaining)
			return nil, ctx.Err()
		case <-t.done:
			cleanup(remaining)
			return nil, t.closedError()
		}
	}
	return resps, nil
}

// Notify implements [Transport].
func (t *StreamTransport) Notify(ctx context.Context, req *Request) error {
	select {
	case <-t.done:
		return t.closedError()
	default:
	}
	return t.write(req)
}

// Close implements [Transport]. It stops the read loop and runs the configured
// close function (if any) exactly once.
func (t *StreamTransport) Close() error {
	t.doneOnce.Do(func() { close(t.done) })
	var err error
	t.closeOnce.Do(func() {
		if t.closeFn != nil {
			err = t.closeFn()
		}
	})
	return err
}

func (t *StreamTransport) closedError() error {
	t.mu.Lock()
	readErr := t.readErr
	t.mu.Unlock()
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return fmt.Errorf("%w: %v", ErrClosed, readErr)
	}
	return ErrClosed
}

// ProcessOption configures a subprocess transport created by
// [NewProcessTransport].
type ProcessOption func(*processConfig)

type processConfig struct {
	stderr          io.Writer
	shutdownTimeout time.Duration
	env             []string // full replacement environment (WithEnv); nil inherits parent
	extraEnv        []string // additional KEY=VALUE merged on top (WithExtraEnv)
	dir             string
	onExit          func(error)
}

// WithStderr routes the child process's standard error to w. By default it is
// inherited from the parent (os.Stderr). Pass io.Discard to silence it.
func WithStderr(w io.Writer) ProcessOption {
	return func(c *processConfig) { c.stderr = w }
}

// WithShutdownTimeout sets how long Close waits for the child to exit after its
// stdin is closed before killing it. The default is 5 seconds.
func WithShutdownTimeout(d time.Duration) ProcessOption {
	return func(c *processConfig) { c.shutdownTimeout = d }
}

// WithEnv sets the environment for the child process (as in os/exec Cmd.Env).
// It replaces the entire environment: every variable the child sees must be
// listed, including PATH, HOME, etc. For the more common case of adding or
// overriding a few variables while keeping the parent environment, use
// [WithExtraEnv] instead.
func WithEnv(env []string) ProcessOption {
	return func(c *processConfig) { c.env = env }
}

// WithExtraEnv adds or overrides the given KEY=VALUE environment variables in
// the child process's environment, on top of the environment it would
// otherwise inherit (the parent's, or the full replacement set by [WithEnv]).
// Entries use os.Environ's "KEY=VALUE" form. Multiple WithExtraEnv options
// accumulate; for a repeated key the last value wins.
//
// Unlike [WithEnv] (which replaces the whole environment), WithExtraEnv keeps
// the parent environment intact and only changes the listed variables — so
// PATH, HOME, etc. are still available to the child.
func WithExtraEnv(kv ...string) ProcessOption {
	return func(c *processConfig) { c.extraEnv = append(c.extraEnv, kv...) }
}

// WithDir sets the working directory for the child process.
func WithDir(dir string) ProcessOption {
	return func(c *processConfig) { c.dir = dir }
}

// WithOnExit registers a callback invoked exactly once when the child process
// exits — whether it exits on its own or as a result of closing the transport.
// The callback receives the error returned by the child's wait (nil for a clean
// exit).
//
// The callback is invoked from a dedicated goroutine after the child has been
// reaped, so it fires even if [StreamTransport.Close] is never called (e.g. the
// child crashes mid-session). It must not block for long and its completion is
// not waited on by Close; do slow cleanup on a separate goroutine.
func WithOnExit(fn func(error)) ProcessOption {
	return func(c *processConfig) { c.onExit = fn }
}

// spawnResult holds the moving parts of a spawned JSON-RPC subprocess shared by
// [NewProcessTransport] (unidirectional) and [NewProcessPeer] (bidirectional).
type spawnResult struct {
	stdin   io.WriteCloser
	stdout  io.Reader
	closeFn func() error
}

// mergeEnv builds the child process environment: base with each entry of extra
// applied (KEY=VALUE), last value winning per key. A nil base means the child
// inherits the parent environment ([os.Environ]); when extra is also empty
// mergeEnv returns nil so the caller can leave [exec.Cmd].Env unset (full
// inheritance). When extra is non-empty a nil base is expanded to os.Environ so
// the listed variables layer on top of the parent environment rather than
// replacing it.
func mergeEnv(base, extra []string) []string {
	if len(extra) == 0 {
		return base // nil when unset -> child inherits parent; or the full replacement
	}
	if base == nil {
		base = os.Environ()
	}
	out := make([]string, 0, len(base)+len(extra))
	idx := make(map[string]int, len(base)+len(extra))
	apply := func(kv string) {
		key := kv
		if i := strings.IndexByte(kv, '='); i >= 0 {
			key = kv[:i]
		}
		if j, ok := idx[key]; ok {
			out[j] = kv // override in place: last value wins
		} else {
			idx[key] = len(out)
			out = append(out, kv)
		}
	}
	for _, kv := range base {
		apply(kv)
	}
	for _, kv := range extra {
		apply(kv)
	}
	return out
}

// spawnProcess starts name with args and returns its stdin/stdout pipes plus a
// close function that closes stdin, waits up to the shutdown timeout for the
// child to exit, then kills it if necessary. A single background goroutine reaps
// the child as soon as it exits — whether spontaneously or because closeFn
// closed stdin — so it never becomes a zombie, and the OnExit hook (if any)
// fires once after the child is reaped. cmd.Wait is called exactly once;
// everything else coordinates through an internal channel.
func spawnProcess(name string, args []string, opts []ProcessOption) (*spawnResult, error) {
	cfg := processConfig{stderr: os.Stderr, shutdownTimeout: 5 * time.Second}
	for _, opt := range opts {
		opt(&cfg)
	}

	cmd := exec.Command(name, args...)
	cmd.Stderr = cfg.stderr
	if merged := mergeEnv(cfg.env, cfg.extraEnv); merged != nil {
		cmd.Env = merged
	}
	if cfg.dir != "" {
		cmd.Dir = cfg.dir
	}

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("jsonrpc: stdin pipe: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("jsonrpc: stdout pipe: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("jsonrpc: start %s: %w", name, err)
	}

	processDone := make(chan error, 1)
	go func() {
		err := cmd.Wait()
		processDone <- err // buffered: the send never blocks, even with no reader
		if cfg.onExit != nil {
			cfg.onExit(err)
		}
	}()

	closeFn := func() error {
		// Closing stdin signals the child (via EOF) to shut down cleanly.
		_ = stdin.Close()
		select {
		case err := <-processDone:
			return err
		case <-time.After(cfg.shutdownTimeout):
			// Child didn't exit gracefully: force-kill, then wait for the
			// reaper goroutine to finish collecting its exit status so we
			// never return before the child is fully reaped.
			_ = cmd.Process.Kill()
			return <-processDone
		}
	}

	return &spawnResult{stdin: stdin, stdout: stdout, closeFn: closeFn}, nil
}

// NewProcessTransport spawns name with args as a JSON-RPC server speaking
// newline-delimited JSON over its stdin/stdout, and returns a transport
// connected to it. Closing the transport closes the child's stdin (signalling
// it to exit), waits up to the shutdown timeout, then kills it if necessary.
// For a bidirectional connection use [NewProcessPeer] instead.
func NewProcessTransport(name string, args []string, opts ...ProcessOption) (*StreamTransport, error) {
	sp, err := spawnProcess(name, args, opts)
	if err != nil {
		return nil, err
	}
	return NewStreamTransport(sp.stdout, sp.stdin, WithCloseFunc(sp.closeFn)), nil
}
