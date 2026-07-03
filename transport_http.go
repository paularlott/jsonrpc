package jsonrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// HTTPTransport is a [Transport] that sends each call as an HTTP POST carrying
// the JSON-RPC request in the body. It is safe for concurrent use.
type HTTPTransport struct {
	url        string
	httpClient *http.Client
	header     http.Header
}

// HTTPOption configures an [HTTPTransport].
type HTTPOption func(*HTTPTransport)

// WithHTTPClient sets the http.Client used for requests. By default
// http.DefaultClient is used.
func WithHTTPClient(client *http.Client) HTTPOption {
	return func(t *HTTPTransport) {
		if client != nil {
			t.httpClient = client
		}
	}
}

// WithHeader adds a header sent on every request (e.g. Authorization). It may
// be called multiple times to add several headers.
func WithHeader(key, value string) HTTPOption {
	return func(t *HTTPTransport) {
		t.header.Add(key, value)
	}
}

// NewHTTPTransport creates an HTTP transport targeting url.
func NewHTTPTransport(url string, opts ...HTTPOption) *HTTPTransport {
	t := &HTTPTransport{
		url:        url,
		httpClient: http.DefaultClient,
		header:     make(http.Header),
	}
	for _, opt := range opts {
		opt(t)
	}
	return t
}

func (t *HTTPTransport) post(ctx context.Context, req *Request) (*http.Response, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("jsonrpc: marshal request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, t.url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("jsonrpc: build request: %w", err)
	}
	for key, values := range t.header {
		for _, v := range values {
			httpReq.Header.Add(key, v)
		}
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")
	return t.httpClient.Do(httpReq)
}

// Call implements [Transport].
func (t *HTTPTransport) Call(ctx context.Context, req *Request) (*Response, error) {
	httpResp, err := t.post(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("jsonrpc: request failed: %w", err)
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(httpResp.Body, 4096))
		return nil, fmt.Errorf("jsonrpc: unexpected status %d: %s", httpResp.StatusCode, bytes.TrimSpace(body))
	}

	var resp Response
	if err := json.NewDecoder(httpResp.Body).Decode(&resp); err != nil {
		return nil, fmt.Errorf("jsonrpc: decode response: %w", err)
	}
	return &resp, nil
}

// CallBatch implements the batch-capable transport extension used by
// [Client.CallBatch]: it POSTs the whole batch as a single JSON-RPC array and
// decodes the array response.
func (t *HTTPTransport) CallBatch(ctx context.Context, reqs []*Request) ([]*Response, error) {
	body, err := json.Marshal(reqs)
	if err != nil {
		return nil, fmt.Errorf("jsonrpc: marshal batch: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, t.url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("jsonrpc: build request: %w", err)
	}
	for key, values := range t.header {
		for _, v := range values {
			httpReq.Header.Add(key, v)
		}
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")

	httpResp, err := t.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("jsonrpc: request failed: %w", err)
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode == http.StatusNoContent {
		// All-notification batch; CallBatch never sends notifications, so an
		// empty response here means the server had nothing to say.
		return nil, nil
	}
	if httpResp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(io.LimitReader(httpResp.Body, 4096))
		return nil, fmt.Errorf("jsonrpc: unexpected status %d: %s", httpResp.StatusCode, bytes.TrimSpace(respBody))
	}

	var resps []*Response
	if err := json.NewDecoder(httpResp.Body).Decode(&resps); err != nil {
		return nil, fmt.Errorf("jsonrpc: decode batch response: %w", err)
	}
	return resps, nil
}

// Notify implements [Transport]. The response body (if any) is drained and
// discarded; a 2xx status is treated as success.
func (t *HTTPTransport) Notify(ctx context.Context, req *Request) error {
	httpResp, err := t.post(ctx, req)
	if err != nil {
		return fmt.Errorf("jsonrpc: request failed: %w", err)
	}
	defer httpResp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(httpResp.Body, 1<<20))
	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		return fmt.Errorf("jsonrpc: unexpected status %d", httpResp.StatusCode)
	}
	return nil
}

// Close implements [Transport]. It is a no-op for HTTP.
func (t *HTTPTransport) Close() error { return nil }
