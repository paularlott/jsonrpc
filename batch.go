package jsonrpc

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

// BatchCall is a single call within a batch request to [Client.CallBatch].
// Out mirrors the out parameter of [Client.Call]: nil discards the result,
// otherwise the result is JSON-decoded into it. Out is only touched from
// CallBatch itself (not concurrently), so it need not be safe for concurrent
// use.
type BatchCall struct {
	Method string
	Params any
	Out    any
}

// BatchResult is the outcome of one [BatchCall] from [Client.CallBatch]. Err is
// non-nil on a transport failure or a JSON-RPC error response (in which case
// it is a *[Error]); Result holds the raw result bytes for callers that did not
// set Out, or who want to inspect the wire value directly.
type BatchResult struct {
	Result json.RawMessage
	Err    error
}

// batchTransport is implemented by transports that can send a whole JSON-RPC
// batch as a single wire message. [HTTPTransport] and [StreamTransport] both
// implement it. Transports that don't are still supported by
// [Client.CallBatch] via a concurrent per-call fallback.
type batchTransport interface {
	CallBatch(ctx context.Context, reqs []*Request) ([]*Response, error)
}

// CallBatch sends every call in a single JSON-RPC 2.0 batch request when the
// underlying [Transport] supports it ([HTTPTransport] and [StreamTransport]
// both do), cutting N round-trips down to one. Transports without native batch
// support still work: CallBatch falls back to issuing the calls concurrently
// over [Transport.Call].
//
// Results are returned in the same order as calls, regardless of transport or
// response ordering. A per-call failure (transport error or JSON-RPC error
// response) is reported in that call's BatchResult.Err; it does not affect
// other calls in the batch. An empty calls slice returns an empty, non-nil
// slice.
func (c *Client) CallBatch(ctx context.Context, calls []BatchCall) []BatchResult {
	results := make([]BatchResult, len(calls))
	if len(calls) == 0 {
		return results
	}

	if bt, ok := c.transport.(batchTransport); ok {
		c.callBatchNative(ctx, bt, calls, results)
		return results
	}

	c.callBatchFallback(ctx, calls, results)
	return results
}

// callBatchNative sends one batch frame via the transport's native support and
// demultiplexes the (possibly reordered) responses back to their call by id.
func (c *Client) callBatchNative(ctx context.Context, bt batchTransport, calls []BatchCall, results []BatchResult) {
	reqs := make([]*Request, len(calls))
	ids := make([]string, len(calls))
	for i, call := range calls {
		encoded, err := encodeParams(call.Params)
		if err != nil {
			results[i] = BatchResult{Err: fmt.Errorf("jsonrpc: encode params: %w", err)}
		}
		id := c.newID()
		ids[i] = string(id)
		reqs[i] = &Request{
			JSONRPC: json.RawMessage(`"2.0"`),
			Method:  call.Method,
			Params:  encoded,
			ID:      id,
		}
	}

	// A per-call encode failure above still occupies a slot in reqs/ids so
	// indices stay aligned; send the batch regardless — the server will simply
	// never see a matching call for that (still well-formed) request.
	resps, err := bt.CallBatch(ctx, reqs)
	if err != nil {
		for i := range results {
			if results[i].Err == nil {
				results[i] = BatchResult{Err: err}
			}
		}
		return
	}

	byID := make(map[string]*Response, len(resps))
	for _, resp := range resps {
		byID[string(resp.ID)] = resp
	}

	for i, call := range calls {
		if results[i].Err != nil {
			continue
		}
		resp, ok := byID[ids[i]]
		if !ok {
			results[i] = BatchResult{Err: fmt.Errorf("jsonrpc: no response for batch call %d (%s)", i, call.Method)}
			continue
		}
		results[i] = decodeBatchResponse(resp, call.Out)
	}
}

// callBatchFallback issues each call concurrently over the transport's regular
// Call method, for transports without native batch framing. It always
// captures the raw result (via an internal json.RawMessage) so BatchResult.Result
// is populated the same way as the native path, in addition to decoding into
// call.Out when set.
func (c *Client) callBatchFallback(ctx context.Context, calls []BatchCall, results []BatchResult) {
	var wg sync.WaitGroup
	for i, call := range calls {
		wg.Add(1)
		go func(i int, call BatchCall) {
			defer wg.Done()
			var raw json.RawMessage
			err := c.Call(ctx, call.Method, call.Params, &raw)
			result := BatchResult{Result: raw, Err: err}
			if err == nil && call.Out != nil && len(raw) > 0 && !isJSONNull(raw) {
				if decErr := json.Unmarshal(raw, call.Out); decErr != nil {
					result.Err = fmt.Errorf("jsonrpc: decode result: %w", decErr)
				}
			}
			results[i] = result
		}(i, call)
	}
	wg.Wait()
}

// decodeBatchResponse turns one raw Response into a BatchResult, decoding into
// out when set and no error is present.
func decodeBatchResponse(resp *Response, out any) BatchResult {
	if resp.Error != nil {
		return BatchResult{Err: resp.Error}
	}
	result := BatchResult{Result: resp.Result}
	if out != nil && len(resp.Result) > 0 && !isJSONNull(resp.Result) {
		if err := json.Unmarshal(resp.Result, out); err != nil {
			return BatchResult{Result: resp.Result, Err: fmt.Errorf("jsonrpc: decode result: %w", err)}
		}
	}
	return result
}
