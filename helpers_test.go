package jsonrpc_test

import (
	"bytes"
	"io"
)

// newReader returns an io.Reader over b, used by decode helpers that need a
// json.Decoder configured with UseNumber.
func newReader(b []byte) io.Reader {
	return bytes.NewReader(b)
}
