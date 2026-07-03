package jsonrpc

import (
	"bytes"
	"encoding/json"
)

// nullID is the id emitted when the inbound id is absent or the request could
// not be parsed far enough to recover its id (per JSON-RPC 2.0).
var nullID = json.RawMessage("null")

// Request is a single inbound JSON-RPC request or notification.
//
// JSONRPC, Params and ID are kept as raw JSON. ID is raw so an absent id
// (notification) is distinguishable from an explicit null and so the original
// id shape round-trips into the response unchanged. Params is raw so handlers
// control their own decoding. JSONRPC is raw so a wrong-typed version field
// yields an Invalid Request error rather than failing the whole batch decode.
type Request struct {
	JSONRPC json.RawMessage `json:"jsonrpc,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
	ID      json.RawMessage `json:"id,omitempty"`
}

// IsNotification reports whether the request is a notification, i.e. it carries
// no id member and therefore expects no response.
func (r *Request) IsNotification() bool {
	return len(r.ID) == 0
}

// hasValidVersion reports whether the jsonrpc member is absent or exactly the
// string "2.0". Absent is tolerated for leniency; any other value is rejected.
func (r *Request) hasValidVersion() bool {
	if len(r.JSONRPC) == 0 {
		return true
	}
	return bytes.Equal(bytes.TrimSpace(r.JSONRPC), []byte(`"2.0"`))
}

// idOrNull returns the request id, or the JSON literal null when absent.
func (r *Request) idOrNull() json.RawMessage {
	if len(r.ID) == 0 {
		return nullID
	}
	return r.ID
}

// Response is a single outbound JSON-RPC response.
//
// Exactly one of Result or Error is meaningful: on success Result holds the
// (already marshalled) result value and Error is nil; on failure Error is set.
// The custom MarshalJSON guarantees the wire form matches the spec — a success
// response always includes a result member (null when the handler returned
// nothing) and never an error member, and vice versa.
type Response struct {
	Result json.RawMessage
	Error  *Error
	ID     json.RawMessage
}

// wireResponse is the concrete on-the-wire shape used for (un)marshalling.
type wireResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *Error          `json:"error,omitempty"`
	ID      json.RawMessage `json:"id"`
}

// MarshalJSON renders the response in spec-compliant form.
func (r Response) MarshalJSON() ([]byte, error) {
	w := wireResponse{JSONRPC: Version, ID: r.ID}
	if len(w.ID) == 0 {
		w.ID = nullID
	}
	if r.Error != nil {
		w.Error = r.Error
		return json.Marshal(w)
	}
	// Success: result member is REQUIRED and must be present even when null.
	if len(r.Result) == 0 {
		w.Result = json.RawMessage("null")
	} else {
		w.Result = r.Result
	}
	return json.Marshal(w)
}

// UnmarshalJSON parses a response from the wire.
func (r *Response) UnmarshalJSON(data []byte) error {
	var w wireResponse
	if err := json.Unmarshal(data, &w); err != nil {
		return err
	}
	r.Result = w.Result
	r.Error = w.Error
	r.ID = w.ID
	return nil
}

// newErrorResponse builds an error response with the given id.
func newErrorResponse(id json.RawMessage, err *Error) *Response {
	return &Response{Error: err, ID: id}
}
