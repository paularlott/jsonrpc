package jsonrpc

import "fmt"

// Error is a JSON-RPC 2.0 error object. It implements the error interface so
// handlers can return it directly and clients receive it from [Client.Call].
//
// Data is optional and, when non-nil, is serialised into the "data" member of
// the error object.
type Error struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

// Error implements the error interface.
func (e *Error) Error() string {
	if e == nil {
		return "<nil jsonrpc error>"
	}
	return fmt.Sprintf("jsonrpc: %s (code %d)", e.Message, e.Code)
}

// NewError builds an [Error] with the given code, message and optional data.
func NewError(code int, message string, data any) *Error {
	return &Error{Code: code, Message: message, Data: data}
}

// Errorf builds an [Error] with a formatted message and no data.
func Errorf(code int, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

// Convenience constructors for the standard error codes. Data is omitted; use
// [NewError] or set the Data field directly when you need to attach details.

// ParseError returns a -32700 error with the given message.
func ParseError(message string) *Error { return &Error{Code: CodeParseError, Message: message} }

// InvalidRequest returns a -32600 error with the given message.
func InvalidRequest(message string) *Error {
	return &Error{Code: CodeInvalidRequest, Message: message}
}

// MethodNotFound returns a -32601 error with the given message.
func MethodNotFound(message string) *Error {
	return &Error{Code: CodeMethodNotFound, Message: message}
}

// InvalidParams returns a -32602 error with the given message.
func InvalidParams(message string) *Error {
	return &Error{Code: CodeInvalidParams, Message: message}
}

// InternalError returns a -32603 error with the given message.
func InternalError(message string) *Error {
	return &Error{Code: CodeInternalError, Message: message}
}

// ServerError returns a -32000 error with the given message.
func ServerError(message string) *Error { return &Error{Code: CodeServerError, Message: message} }

// asError coerces an arbitrary handler error into a *Error. A *Error is passed
// through unchanged; any other error becomes a server error carrying its text.
func asError(err error) *Error {
	if err == nil {
		return nil
	}
	if e, ok := err.(*Error); ok {
		return e
	}
	return &Error{Code: CodeServerError, Message: err.Error()}
}
