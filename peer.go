package jsonrpc

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
)

// Peer is a bidirectional JSON-RPC 2.0 connection over a pair of byte streams.
// Unlike a [Client] (outbound only) or a [Server] served via
// [Server.ServeStream] (inbound only), a Peer does both at once over the same
// stream: it sends outbound requests and correlates their responses by id, AND
// it receives inbound requests, dispatches them to a method registry, and
// writes the responses back.
//
// This is the shape protocols use when both ends act as both client and server
// on a single connection — for example a host that calls methods on a spawned
// plugin while also serving "host callback" methods the plugin invokes back
// (remote objects, progress, elicitation).
//
// NewPeer starts a background reader immediately. The reader demultiplexes each
// incoming message, routing responses to pending outbound calls and requests to
// the Peer's [Server]. Inbound request handlers run in their own goroutine so a
// slow handler cannot stall delivery of outbound responses. Call [Peer.Close]
// to stop the reader and release the streams. Outbound calls go through the
// Peer's [Client] (see [Peer.Client]).
type Peer struct {
	server *Server
	client *Client

	in      io.Reader
	w       io.Writer
	writeMu sync.Mutex

	mu      sync.Mutex
	pending map[string]chan *Response
	readErr error

	// handlerWG tracks in-flight inbound-handler goroutines so [Peer.Wait] can
	// block until every response has been written — the server-side grace a
	// caller needs before treating the Peer as quiesced (e.g. on EOF).
	handlerWG sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc

	done      chan struct{}
	doneOnce  sync.Once
	closeOnce sync.Once
	closeFn   func() error
}

// PeerOption configures a [Peer].
type PeerOption func(*Peer)

// WithPeerCloseFunc registers a function run once when the Peer is closed, after
// the reader is signalled to stop. Use it to release resources backing the
// streams (for example, close a subprocess's stdin and reap it via
// [NewProcessTransport]'s close function).
func WithPeerCloseFunc(fn func() error) PeerOption {
	return func(p *Peer) { p.closeFn = fn }
}

// NewPeer returns a Peer that reads JSON-RPC messages from in and writes them
// to out, dispatching inbound requests to server (pass [NewServer] if you have
// no existing registry) and correlating outbound call responses by id.
//
// NewPeer does not start the reader — call [Peer.Serve] from the goroutine that
// should own the input. That lets a caller finish wiring up state the handlers
// depend on (for example, pointing a server runtime at the peer) before any
// handler can run, avoiding a construction race.
func NewPeer(in io.Reader, out io.Writer, server *Server, opts ...PeerOption) *Peer {
	ctx, cancel := context.WithCancel(context.Background())
	p := &Peer{
		server:  server,
		in:      in,
		w:       out,
		pending: make(map[string]chan *Response),
		ctx:     ctx,
		cancel:  cancel,
		done:    make(chan struct{}),
	}
	for _, opt := range opts {
		opt(p)
	}
	p.client = NewClient(p)
	return p
}

// Serve runs the read loop until the input reaches EOF or a fatal decode error,
// returning nil for a clean EOF and the error otherwise. Serve blocks; inbound
// handlers run concurrently in their own goroutines. Call [Peer.Wait] after
// Serve returns to block until any still-running handlers have flushed.
func (p *Peer) Serve() error {
	p.readLoop(p.in)
	if err := p.Err(); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}

// NewProcessPeer spawns name with args as a JSON-RPC 2.0 peer speaking
// newline-delimited JSON over its stdin/stdout, and returns a bidirectional
// [Peer] connected to it. It is the subprocess counterpart to [NewPeer], just as
// [NewProcessTransport] is the subprocess counterpart to [NewStreamTransport]:
// use it when the child both handles requests and makes reverse-direction
// requests (host callbacks, remote objects, elicitation).
//
// It accepts the same options as [NewProcessTransport] ([WithStderr],
// [WithShutdownTimeout], [WithEnv], [WithDir], [WithOnExit]). Closing the Peer
// closes the child's stdin, waits up to the shutdown timeout, then kills it if
// necessary; the OnExit hook fires once after the child is reaped.
func NewProcessPeer(name string, args []string, server *Server, opts ...ProcessOption) (*Peer, error) {
	sp, err := spawnProcess(name, args, opts)
	if err != nil {
		return nil, err
	}
	p := NewPeer(sp.stdout, sp.stdin, server, WithPeerCloseFunc(sp.closeFn))
	// Client convenience: start the background reader so the returned Peer can
	// be called immediately, matching NewProcessTransport.
	go func() { _ = p.Serve() }()
	return p, nil
}

// Server returns the inbound method registry. Handlers may be registered before
// or after NewPeer.
func (p *Peer) Server() *Server { return p.server }

// Client returns the outbound client used to make requests and send
// notifications that the other end will serve:
//
//	var result T
//	if err := peer.Client().Call(ctx, "remote.method", params, &result); err != nil { ... }
//
// The client shares the Peer's connection and is safe for concurrent use.
func (p *Peer) Client() *Client { return p.client }

// Handle registers an inbound handler on the Peer's [Server]; it is a
// convenience for p.Server().Handle. It returns the server for chaining.
func (p *Peer) Handle(method string, h Handler) *Server {
	return p.server.Handle(method, h)
}

// Unregister removes an inbound handler; it is a convenience for
// p.Server().Unregister.
func (p *Peer) Unregister(method string) bool { return p.server.Unregister(method) }

// Done returns a channel that is closed when the reader exits — on EOF from in,
// a fatal decode error, or Close. Err returns the reason (nil for a clean EOF).
func (p *Peer) Done() <-chan struct{} { return p.done }

// Wait blocks until every in-flight inbound handler has finished, so any
// response still being written when the reader exited is flushed before Wait
// returns. Call it after <-Done (or Close) when serving must not return before
// all handlers are quiesced — for example a stdio server exiting on EOF.
func (p *Peer) Wait() { p.handlerWG.Wait() }

// Err returns the read error that stopped the reader, or nil if it ended
// cleanly (EOF) or the Peer was closed without a read error.
func (p *Peer) Err() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.readErr
}

// Close stops the read loop, cancels contexts passed to in-flight inbound
// handlers, and runs the configured close function (if any) exactly once.
// Pending and future outbound calls fail.
func (p *Peer) Close() error {
	p.cancel()
	p.doneOnce.Do(func() { close(p.done) })
	var err error
	p.closeOnce.Do(func() {
		if p.closeFn != nil {
			err = p.closeFn()
		}
	})
	return err
}

// Call implements [Transport] for the Peer's internal [Client]. It is exported
// only to satisfy the interface; callers invoke outbound calls via
// [Peer.Client].
func (p *Peer) Call(ctx context.Context, req *Request) (*Response, error) {
	key := string(bytes.TrimSpace(req.ID))
	ch := make(chan *Response, 1)

	p.mu.Lock()
	select {
	case <-p.done:
		p.mu.Unlock()
		return nil, p.closedError()
	default:
	}
	if _, exists := p.pending[key]; exists {
		p.mu.Unlock()
		return nil, fmt.Errorf("jsonrpc: duplicate in-flight request id %s", key)
	}
	p.pending[key] = ch
	p.mu.Unlock()

	if err := p.write(req); err != nil {
		p.mu.Lock()
		delete(p.pending, key)
		p.mu.Unlock()
		return nil, err
	}

	select {
	case resp := <-ch:
		return resp, nil
	case <-ctx.Done():
		p.mu.Lock()
		delete(p.pending, key)
		p.mu.Unlock()
		return nil, ctx.Err()
	case <-p.done:
		return nil, p.closedError()
	}
}

// CallBatch implements the batch-capable transport extension used by
// [Client.CallBatch]: it writes the whole batch as a single JSON-RPC array line
// and collects each response by id as it arrives.
func (p *Peer) CallBatch(ctx context.Context, reqs []*Request) ([]*Response, error) {
	if len(reqs) == 0 {
		return nil, nil
	}

	keys := make([]string, len(reqs))
	chans := make(map[string]chan *Response, len(reqs))

	p.mu.Lock()
	select {
	case <-p.done:
		p.mu.Unlock()
		return nil, p.closedError()
	default:
	}
	for i, req := range reqs {
		key := string(bytes.TrimSpace(req.ID))
		keys[i] = key
		if _, exists := p.pending[key]; exists {
			p.mu.Unlock()
			return nil, fmt.Errorf("jsonrpc: duplicate in-flight request id %s", key)
		}
		ch := make(chan *Response, 1)
		p.pending[key] = ch
		chans[key] = ch
	}
	p.mu.Unlock()

	cleanup := func(remaining map[string]chan *Response) {
		p.mu.Lock()
		for key := range remaining {
			delete(p.pending, key)
		}
		p.mu.Unlock()
	}

	if err := p.write(reqs); err != nil {
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
		case <-p.done:
			cleanup(remaining)
			return nil, p.closedError()
		}
	}
	return resps, nil
}

// Notify implements [Transport].
func (p *Peer) Notify(ctx context.Context, req *Request) error {
	select {
	case <-p.done:
		return p.closedError()
	default:
	}
	if err := p.write(req); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func (p *Peer) write(v any) error {
	body, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("jsonrpc: marshal: %w", err)
	}
	body = append(body, '\n')
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	if _, err := p.w.Write(body); err != nil {
		p.fail(fmt.Errorf("jsonrpc: write: %w", err))
		return err
	}
	return nil
}

func (p *Peer) deliver(resp *Response) {
	if resp == nil {
		return
	}
	key := string(bytes.TrimSpace(resp.ID))
	p.mu.Lock()
	ch, ok := p.pending[key]
	if ok {
		delete(p.pending, key)
	}
	p.mu.Unlock()
	if ok {
		ch <- resp
	}
}

func (p *Peer) fail(err error) {
	p.mu.Lock()
	// Prefer an informative error over a plain EOF: a write or decode failure
	// that precedes (or follows) the reader's EOF should win so callers see the
	// real cause rather than a benign EOF.
	if p.readErr == nil || (errors.Is(p.readErr, io.EOF) && !errors.Is(err, io.EOF)) {
		p.readErr = err
	}
	p.mu.Unlock()
	p.doneOnce.Do(func() { close(p.done) })
}

func (p *Peer) closedError() error {
	p.mu.Lock()
	readErr := p.readErr
	p.mu.Unlock()
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return fmt.Errorf("%w: %v", ErrClosed, readErr)
	}
	return ErrClosed
}

// readLoop owns the input stream. It decodes newline- or whitespace-delimited
// JSON values and routes each one: a response (carrying result or error) is
// delivered to the pending outbound call waiting on its id; a request or
// notification (carrying a method) is dispatched via the Server. Inbound
// requests run in their own goroutine so a slow handler cannot stall the reader
// or delay outbound response delivery.
func (p *Peer) readLoop(in io.Reader) {
	decoder := json.NewDecoder(bufio.NewReader(in))
	for {
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			p.fail(err)
			return
		}
		trimmed := bytes.TrimSpace(raw)
		if len(trimmed) == 0 {
			continue
		}
		if trimmed[0] == '[' {
			var elems []json.RawMessage
			if err := json.Unmarshal(raw, &elems); err != nil {
				continue // skip malformed batch framing
			}
			if isResponseBatch(elems) {
				// A batch of responses to our outbound CallBatch: deliver each.
				for _, elem := range elems {
					p.handleMessage(elem)
				}
				continue
			}
			// Request batch: let the server dispatch it as one batch so the
			// responses are written as a single JSON-RPC array (matching the
			// batch framing clients expect), the same as [Server.ServeStream].
			out, hasResp := p.server.HandleMessage(p.ctx, raw)
			if hasResp {
				_ = p.write(json.RawMessage(out))
			}
			continue
		}
		p.handleMessage(raw)
	}
}

// isResponseBatch reports whether every element of a batch is a response (no
// method member). A batch with any request element is treated as a request
// batch and dispatched as a whole.
func isResponseBatch(elems []json.RawMessage) bool {
	for _, elem := range elems {
		var probe peerMessage
		if json.Unmarshal(elem, &probe) != nil {
			continue
		}
		if probe.Method != "" {
			return false
		}
	}
	return true
}

// peerMessage is the superset of request and response fields used to classify
// an inbound message in a single decode.
type peerMessage struct {
	JSONRPC json.RawMessage `json:"jsonrpc,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *Error          `json:"error,omitempty"`
}

// handleMessage classifies one decoded message. Per JSON-RPC 2.0 a request (or
// notification) carries a method member; anything without a method is a
// response and is delivered to its pending outbound call. A response is
// identified by the absence of a method rather than the presence of result or
// error, so void responses (no result, no error — just an id) are still
// delivered correctly.
func (p *Peer) handleMessage(raw json.RawMessage) {
	var msg peerMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		// Malformed JSON object: let the server produce an Invalid Request
		// response with a null id rather than dropping it silently.
		p.handlerWG.Add(1)
		go func() {
			defer p.handlerWG.Done()
			p.dispatchRequest(&Request{})
		}()
		return
	}
	if msg.Method == "" {
		p.deliver(&Response{Result: msg.Result, Error: msg.Error, ID: msg.ID})
		return
	}
	// Run in its own goroutine so a handler making a nested (reverse-direction)
	// call, or simply a slow handler, cannot stall the reader and delay
	// delivery of responses to pending outbound calls.
	p.handlerWG.Add(1)
	go func() {
		defer p.handlerWG.Done()
		p.dispatchRequest(&Request{
			JSONRPC: msg.JSONRPC,
			Method:  msg.Method,
			Params:  msg.Params,
			ID:      msg.ID,
		})
	}()
}

// dispatchRequest runs an inbound request through the server and writes the
// response (if one is due) back on the stream. It runs in its own goroutine so
// the reader is never blocked by a handler.
func (p *Peer) dispatchRequest(req *Request) {
	resp := p.server.HandleRequest(p.ctx, req)
	if resp == nil {
		return // notification, or otherwise no response due
	}
	_ = p.write(resp)
}
