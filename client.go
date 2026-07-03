package jsonrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync/atomic"
)

// Transport is the client-side connection to a JSON-RPC endpoint. A single
// Transport may be shared by concurrent [Client] calls; implementations must be
// safe for concurrent use.
type Transport interface {
	// Call sends a request and returns the correlated response. The request
	// always carries an id. Implementations must return a non-nil *Response on
	// success (which may itself carry a JSON-RPC Error), or a non-nil error for
	// transport-level failures.
	Call(ctx context.Context, req *Request) (*Response, error)
	// Notify sends a notification (a request with no id) for which no response
	// is expected. It returns only transport-level errors.
	Notify(ctx context.Context, req *Request) error
	// Close releases any resources held by the transport.
	Close() error
}

// Client is a JSON-RPC 2.0 client. It is a thin, transport-agnostic wrapper
// that generates request ids, encodes params, and decodes results. It is safe
// for concurrent use if the underlying [Transport] is.
type Client struct {
	transport Transport
	nextID    atomic.Uint64
}

// NewClient wraps a [Transport] in a client.
func NewClient(t Transport) *Client {
	return &Client{transport: t}
}

// Call invokes method with params and decodes the result into out.
//
// params is encoded to JSON (a nil params sends no params member; a
// json.RawMessage is sent verbatim). out may be nil to discard the result;
// otherwise the result is JSON-decoded into out. A JSON-RPC error response is
// returned as a *[Error].
func (c *Client) Call(ctx context.Context, method string, params, out any) error {
	encoded, err := encodeParams(params)
	if err != nil {
		return fmt.Errorf("jsonrpc: encode params: %w", err)
	}
	req := &Request{
		JSONRPC: json.RawMessage(`"2.0"`),
		Method:  method,
		Params:  encoded,
		ID:      c.newID(),
	}
	resp, err := c.transport.Call(ctx, req)
	if err != nil {
		return err
	}
	if resp.Error != nil {
		return resp.Error
	}
	if out != nil && len(resp.Result) > 0 && !isJSONNull(resp.Result) {
		if err := json.Unmarshal(resp.Result, out); err != nil {
			return fmt.Errorf("jsonrpc: decode result: %w", err)
		}
	}
	return nil
}

// Notify sends a notification: a method call for which no response is expected.
func (c *Client) Notify(ctx context.Context, method string, params any) error {
	encoded, err := encodeParams(params)
	if err != nil {
		return fmt.Errorf("jsonrpc: encode params: %w", err)
	}
	req := &Request{
		JSONRPC: json.RawMessage(`"2.0"`),
		Method:  method,
		Params:  encoded,
	}
	return c.transport.Notify(ctx, req)
}

// Close closes the underlying transport.
func (c *Client) Close() error {
	return c.transport.Close()
}

func (c *Client) newID() json.RawMessage {
	id := c.nextID.Add(1)
	return json.RawMessage(strconv.FormatUint(id, 10))
}

func encodeParams(params any) (json.RawMessage, error) {
	if params == nil {
		return nil, nil
	}
	if raw, ok := params.(json.RawMessage); ok {
		return raw, nil
	}
	return json.Marshal(params)
}

func isJSONNull(raw json.RawMessage) bool {
	return bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}
