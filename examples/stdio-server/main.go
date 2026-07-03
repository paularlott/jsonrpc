// Command stdio-server is a JSON-RPC 2.0 server that speaks newline-delimited
// JSON over stdin/stdout. It is the shape of server a parent process launches
// as a subprocess (as MCP does for stdio servers).
//
// Try it directly by piping a request in:
//
//	echo '{"jsonrpc":"2.0","method":"greet","params":{"name":"Ada"},"id":1}' | go run ./examples/stdio-server
//
// Or drive it from the stdio-client example.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/paularlott/jsonrpc"
)

func main() {
	// Servers that talk over stdout must never write anything but protocol
	// frames to stdout; logs go to stderr.
	log.SetOutput(os.Stderr)

	srv := jsonrpc.NewServer()

	srv.Handle("greet", jsonrpc.Typed(func(ctx context.Context, p struct {
		Name string `json:"name"`
	}) (any, error) {
		if p.Name == "" {
			return nil, jsonrpc.InvalidParams("name is required")
		}
		return "Hello, " + p.Name + "!", nil
	}))

	srv.Handle("upper", func(ctx context.Context, params json.RawMessage) (any, error) {
		var s string
		if err := jsonrpc.Unmarshal(params, &s); err != nil {
			return nil, err
		}
		return strings.ToUpper(s), nil
	})

	srv.Handle("log", func(ctx context.Context, params json.RawMessage) (any, error) {
		// Typically invoked as a notification; log to stderr and return nothing.
		var msg string
		_ = jsonrpc.Unmarshal(params, &msg)
		fmt.Fprintf(os.Stderr, "[client log] %s\n", msg)
		return nil, nil
	})

	if err := srv.ServeStream(context.Background(), os.Stdin, os.Stdout); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
