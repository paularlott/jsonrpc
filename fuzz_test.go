package jsonrpc_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/paularlott/jsonrpc"
)

// FuzzHandleMessage throws arbitrary bytes at the dispatcher and asserts two
// invariants that must hold for any input: the call never panics, and whenever
// a response is produced it is well-formed JSON. Run with:
//
//	go test -run=x -fuzz=FuzzHandleMessage
func FuzzHandleMessage(f *testing.F) {
	seeds := []string{
		`{"jsonrpc":"2.0","method":"echo","params":[1,2,3],"id":1}`,
		`{"jsonrpc":"2.0","method":"echo"}`,
		`[{"jsonrpc":"2.0","method":"echo","id":1},{"jsonrpc":"2.0","method":"echo"}]`,
		`[]`,
		`[1,2,3]`,
		`{"jsonrpc":"1.0","method":"echo","id":1}`,
		`not json`,
		``,
		`   `,
		`{"jsonrpc":"2.0","method":123,"id":1}`,
		`{"jsonrpc":"2.0","method":"echo","params":{"big":9007199254740993},"id":"x"}`,
		`null`,
		`42`,
		`"a string"`,
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}

	srv := jsonrpc.NewServer()
	srv.Handle("echo", func(ctx context.Context, params json.RawMessage) (any, error) {
		return params, nil
	})
	srv.Handle("boom", func(ctx context.Context, params json.RawMessage) (any, error) {
		panic("boom")
	})

	f.Fuzz(func(t *testing.T, data []byte) {
		out, has := srv.HandleMessage(context.Background(), data)
		if has && !json.Valid(out) {
			t.Fatalf("produced invalid JSON response %q for input %q", out, data)
		}
		if !has && len(out) != 0 {
			t.Fatalf("no-response signalled but bytes returned: %q", out)
		}
	})
}
