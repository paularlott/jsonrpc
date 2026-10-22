package jsonrpc

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
)

// Server is a JSON-RPC 2.0 method registry and dispatcher. It is safe for
// concurrent use: handlers may be registered before serving, and dispatch is
// concurrent by default. The zero value is not usable; create one with
// [NewServer].
//
// A Server is transport-agnostic. [Server.ServeHTTP] exposes it as an
// http.Handler, [Server.ServeStream] runs the newline-delimited loop used for
// stdio, and [Server.HandleMessage] / [Server.HandleRequest] expose the
// dispatch core directly for embedding in other transports.
type Server struct {
	mu      sync.RWMutex
	methods map[string]Handler

	onError  func(method string, err error)
	serial   bool
	maxBatch int           // 0 = no limit, see WithMaxBatch
	slots    chan struct{} // nil = no limit, see WithMaxConcurrency
}

// Option configures a [Server].
type Option func(*Server)

// WithErrorHandler registers a hook invoked whenever a handler returns a
// non-nil error or panics — including for notifications, whose errors are
// otherwise invisible. It is intended for logging and must not block.
func WithErrorHandler(fn func(method string, err error)) Option {
	return func(s *Server) { s.onError = fn }
}

// WithSerialDispatch disables concurrent dispatch. Batch elements and stream
// messages are then processed one at a time in arrival order. By default the
// server dispatches each message (and each batch element) concurrently.
func WithSerialDispatch() Option {
	return func(s *Server) { s.serial = true }
}

// WithMaxBatch limits a batch to n elements. A larger batch is answered with a
// single Invalid Request error and none of its elements run. A batch element
// is tiny, so a request-size limit alone still lets one request carry
// hundreds of thousands of calls. n <= 0 means no limit (the default).
func WithMaxBatch(n int) Option {
	return func(s *Server) { s.maxBatch = n }
}

// WithMaxConcurrency limits how many handlers run at once across the whole
// server: batch elements, single requests and stream messages alike. Further
// calls wait for a free slot (or for their context to end), and a batch or a
// stream starts no more goroutines than the limit, so neither one large batch
// nor many concurrent requests can run more than n handlers. n <= 0 means no
// limit (the default).
func WithMaxConcurrency(n int) Option {
	return func(s *Server) {
		if n > 0 {
			s.slots = make(chan struct{}, n)
		} else {
			s.slots = nil
		}
	}
}

// NewServer creates a Server with no methods registered.
func NewServer(opts ...Option) *Server {
	s := &Server{methods: make(map[string]Handler)}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Handle registers h for the given method name, replacing any existing handler.
// It returns the server to allow chaining. It panics if h is nil.
func (s *Server) Handle(method string, h Handler) *Server {
	if h == nil {
		panic("jsonrpc: nil handler for method " + method)
	}
	s.mu.Lock()
	s.methods[method] = h
	s.mu.Unlock()
	return s
}

// Unregister removes the handler for method, if any. It reports whether a
// handler was present.
func (s *Server) Unregister(method string) bool {
	s.mu.Lock()
	_, ok := s.methods[method]
	delete(s.methods, method)
	s.mu.Unlock()
	return ok
}

// Methods returns the sorted-order-independent set of registered method names.
func (s *Server) Methods() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	names := make([]string, 0, len(s.methods))
	for name := range s.methods {
		names = append(names, name)
	}
	return names
}

// HandleRequest dispatches a single already-decoded request. It returns the
// response to send, or nil when no response is due (i.e. the request is a
// notification, valid or not). This is the lowest-level dispatch entry point;
// most callers use [Server.HandleMessage].
func (s *Server) HandleRequest(ctx context.Context, req *Request) *Response {
	// Structural validation happens before the notification check: an invalid
	// Request object is not a valid notification, so per JSON-RPC 2.0 it must
	// receive an error response with a null id even when no id is present.
	if !req.hasValidVersion() {
		return newErrorResponse(req.idOrNull(), InvalidRequest(`jsonrpc must be "2.0"`))
	}
	if req.Method == "" {
		return newErrorResponse(req.idOrNull(), InvalidRequest("missing method"))
	}

	notification := req.IsNotification()

	s.mu.RLock()
	h, ok := s.methods[req.Method]
	s.mu.RUnlock()
	if !ok {
		if notification {
			// Unknown notifications are silently ignored per JSON-RPC 2.0.
			return nil
		}
		return newErrorResponse(req.idOrNull(), MethodNotFound("method not found: "+req.Method))
	}

	result, err := s.invoke(withNotification(ctx, notification), req.Method, h, req.Params)
	if notification {
		return nil
	}
	if err != nil {
		return newErrorResponse(req.idOrNull(), asError(err))
	}

	raw, merr := marshalResult(result)
	if merr != nil {
		return newErrorResponse(req.idOrNull(), InternalError("failed to marshal result: "+merr.Error()))
	}
	return &Response{Result: raw, ID: req.idOrNull()}
}

// invoke runs a handler with panic recovery, reporting errors and panics to the
// error hook when configured. Under WithMaxConcurrency it first waits for a
// free slot; a context that ends while waiting is the call's error.
func (s *Server) invoke(ctx context.Context, method string, h Handler, params json.RawMessage) (result any, err error) {
	if s.slots != nil {
		select {
		case s.slots <- struct{}{}:
			defer func() { <-s.slots }()
		case <-ctx.Done():
			err = InternalError("server busy: " + ctx.Err().Error())
			if s.onError != nil {
				s.onError(method, err)
			}
			return nil, err
		}
	}
	defer func() {
		if r := recover(); r != nil {
			err = InternalError(fmt.Sprintf("handler panic: %v", r))
			result = nil
			if s.onError != nil {
				s.onError(method, err)
			}
		}
	}()
	result, err = h(ctx, params)
	if err != nil && s.onError != nil {
		s.onError(method, err)
	}
	return result, err
}

// marshalResult marshals a handler result. A nil result becomes JSON null.
func marshalResult(result any) (json.RawMessage, error) {
	if result == nil {
		return json.RawMessage("null"), nil
	}
	// Allow handlers to return pre-encoded JSON verbatim.
	if raw, ok := result.(json.RawMessage); ok {
		if len(raw) == 0 {
			return json.RawMessage("null"), nil
		}
		return raw, nil
	}
	return json.Marshal(result)
}

// HandleMessage dispatches one raw inbound JSON-RPC message — a single request,
// a single notification, or a batch array. It returns the marshalled response
// and whether a response is due. A notification (or an all-notification batch)
// yields (nil, false); every other message yields (bytes, true), including
// parse and invalid-request errors.
func (s *Server) HandleMessage(ctx context.Context, raw []byte) ([]byte, bool) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return mustMarshal(newErrorResponse(nullID, ParseError("empty request"))), true
	}
	if !json.Valid(trimmed) {
		return mustMarshal(newErrorResponse(nullID, ParseError("invalid JSON"))), true
	}
	if trimmed[0] == '[' {
		return s.handleBatch(ctx, trimmed)
	}
	return s.handleSingle(ctx, trimmed)
}

func (s *Server) handleSingle(ctx context.Context, raw []byte) ([]byte, bool) {
	var req Request
	if err := json.Unmarshal(raw, &req); err != nil {
		// Valid JSON, but not a Request object (e.g. a bare number or string).
		return mustMarshal(newErrorResponse(nullID, InvalidRequest(err.Error()))), true
	}
	resp := s.HandleRequest(ctx, &req)
	if resp == nil {
		return nil, false
	}
	return mustMarshal(resp), true
}

func (s *Server) handleBatch(ctx context.Context, raw []byte) ([]byte, bool) {
	var elems []json.RawMessage
	if err := json.Unmarshal(raw, &elems); err != nil {
		return mustMarshal(newErrorResponse(nullID, ParseError(err.Error()))), true
	}
	if len(elems) == 0 {
		return mustMarshal(newErrorResponse(nullID, InvalidRequest("empty batch"))), true
	}
	if s.maxBatch > 0 && len(elems) > s.maxBatch {
		return mustMarshal(newErrorResponse(nullID, InvalidRequest(
			fmt.Sprintf("batch too large: %d elements exceeds the limit of %d", len(elems), s.maxBatch)))), true
	}
	responses := s.dispatchBatch(ctx, elems)
	if len(responses) == 0 {
		return nil, false
	}
	return mustMarshal(responses), true
}

// dispatchBatch processes every element of a batch, returning only the
// responses that are due (notifications contribute nothing). Order of the
// returned slice is not significant per JSON-RPC 2.0.
func (s *Server) dispatchBatch(ctx context.Context, elems []json.RawMessage) []*Response {
	if s.serial {
		responses := make([]*Response, 0, len(elems))
		for _, elem := range elems {
			if resp := s.dispatchElement(ctx, elem); resp != nil {
				responses = append(responses, resp)
			}
		}
		return responses
	}

	var (
		mu        sync.Mutex
		responses []*Response
		wg        sync.WaitGroup
	)
	// Under WithMaxConcurrency a fixed set of workers drains the batch, so it
	// never starts more goroutines than handlers may run at once.
	workers := len(elems)
	if s.slots != nil && cap(s.slots) < workers {
		workers = cap(s.slots)
	}
	next := make(chan json.RawMessage, len(elems))
	for _, elem := range elems {
		next <- elem
	}
	close(next)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for elem := range next {
				if resp := s.dispatchElement(ctx, elem); resp != nil {
					mu.Lock()
					responses = append(responses, resp)
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	return responses
}

func (s *Server) dispatchElement(ctx context.Context, elem json.RawMessage) *Response {
	var req Request
	if err := json.Unmarshal(elem, &req); err != nil {
		// A batch element that is valid JSON but not a Request object still
		// gets an individual error response (per the spec's [1,2,3] example).
		return newErrorResponse(nullID, InvalidRequest(err.Error()))
	}
	return s.HandleRequest(ctx, &req)
}

// ServeHTTP exposes the server as an http.Handler. It accepts POST requests
// carrying a single JSON-RPC object or a batch array. A message that produces
// no response (a notification, or an all-notification batch) yields 204 No
// Content; otherwise the JSON response is returned with a 200 status.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}
	out, hasResponse := s.HandleMessage(r.Context(), body)
	if !hasResponse {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(out)
}

// ServeStream runs a newline-delimited JSON-RPC loop over the given streams,
// the transport used for stdio servers. It reads JSON values from in (they need
// not be newline-separated; any JSON whitespace works) and writes each response
// as a single line to out. Requests are dispatched concurrently by default and
// writes are serialised.
//
// The call blocks until in reaches EOF (returning the first write error seen,
// if any) or a fatal decode error occurs. A decode error means the stream can
// no longer be framed reliably, so a parse-error response is emitted and the
// error is returned. Cancelling ctx cancels the context passed to in-flight
// handlers but does not itself unblock a pending read; close in to stop the
// loop.
func (s *Server) ServeStream(ctx context.Context, in io.Reader, out io.Writer) error {
	decoder := json.NewDecoder(bufio.NewReader(in))
	var inFlight chan struct{} // bounds messages in flight under WithMaxConcurrency
	if s.slots != nil {
		inFlight = make(chan struct{}, cap(s.slots))
	}

	var (
		writeMu  sync.Mutex
		wg       sync.WaitGroup
		firstErr error
		errMu    sync.Mutex
	)
	recordErr := func(err error) {
		if err == nil {
			return
		}
		errMu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		errMu.Unlock()
	}
	writeLine := func(b []byte) {
		writeMu.Lock()
		_, err := out.Write(append(b, '\n'))
		writeMu.Unlock()
		recordErr(err)
	}

	for {
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			if err == io.EOF {
				wg.Wait()
				errMu.Lock()
				defer errMu.Unlock()
				return firstErr
			}
			// Unrecoverable stream corruption: framing is lost, so emit a
			// parse error and stop.
			writeLine(mustMarshal(newErrorResponse(nullID, ParseError(err.Error()))))
			wg.Wait()
			return err
		}

		if len(bytes.TrimSpace(raw)) == 0 {
			continue
		}

		process := func(msg json.RawMessage) {
			defer wg.Done()
			if inFlight != nil {
				defer func() { <-inFlight }()
			}
			if resp, hasResponse := s.HandleMessage(ctx, msg); hasResponse {
				writeLine(resp)
			}
		}

		wg.Add(1)
		if s.serial {
			process(raw)
		} else {
			if inFlight != nil {
				// Wait for a free slot before reading on, so a flood of
				// messages queues in the stream rather than in goroutines.
				inFlight <- struct{}{}
			}
			go process(raw)
		}
	}
}

// mustMarshal marshals a response (or slice of responses) to JSON, falling back
// to an internal-error response if marshalling somehow fails.
func mustMarshal(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		b, _ = json.Marshal(newErrorResponse(nullID, InternalError("failed to marshal response: "+err.Error())))
	}
	return b
}
