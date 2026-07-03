// Package jsonrpc implements the JSON-RPC 2.0 specification with pluggable
// transports.
//
// The package is transport-agnostic at its core: a [Server] dispatches decoded
// requests to registered handlers, and the same dispatch logic backs the HTTP
// handler ([Server.ServeHTTP]) and the newline-delimited stream loop
// ([Server.ServeStream], suitable for stdio). A [Client] talks to a remote
// endpoint over any [Transport] — HTTP ([HTTPTransport]), an arbitrary
// byte stream ([StreamTransport]), or a spawned subprocess ([NewProcessTransport]).
// When both ends must act as client and server on a single stream, a [Peer]
// combines the two ([NewPeer], or [NewProcessPeer] for a subprocess).
//
// The wire format follows JSON-RPC 2.0 exactly: request/notification framing,
// batch arrays, notification (no id) semantics, and the standard error codes.
// Request ids are preserved as raw JSON so their original shape (number, string
// or null) round-trips into the response unchanged, and request params are kept
// as [encoding/json.RawMessage] so handlers control their own decoding
// (preserving integer precision, custom number handling, etc.).
package jsonrpc

// Version is the JSON-RPC protocol version emitted on every response and
// required (when present) on every request.
const Version = "2.0"

// Standard JSON-RPC 2.0 error codes.
//
// Codes from -32000 to -32099 are reserved for implementation-defined server
// errors; [CodeServerError] is the conventional default within that range.
const (
	// CodeParseError indicates invalid JSON was received by the server.
	CodeParseError = -32700
	// CodeInvalidRequest indicates the JSON sent is not a valid Request object.
	CodeInvalidRequest = -32600
	// CodeMethodNotFound indicates the method does not exist or is not available.
	CodeMethodNotFound = -32601
	// CodeInvalidParams indicates invalid method parameters.
	CodeInvalidParams = -32602
	// CodeInternalError indicates an internal JSON-RPC error.
	CodeInternalError = -32603
	// CodeServerError is the conventional code for an application/server error
	// (reserved range -32000 to -32099).
	CodeServerError = -32000
)
