// Command http-server exposes a small calculator over JSON-RPC 2.0 on HTTP.
//
//	go run ./examples/http-server
//
// Then, in another terminal:
//
//	curl -s localhost:8080/rpc -d '{"jsonrpc":"2.0","method":"add","params":{"a":2,"b":40},"id":1}'
//	curl -s localhost:8080/rpc -d '[{"jsonrpc":"2.0","method":"add","params":{"a":1,"b":1},"id":1},{"jsonrpc":"2.0","method":"mul","params":{"a":6,"b":7},"id":2}]'
package main

import (
	"context"
	"log"
	"net/http"

	"github.com/paularlott/jsonrpc"
)

type pair struct {
	A float64 `json:"a"`
	B float64 `json:"b"`
}

func main() {
	rpc := jsonrpc.NewServer(jsonrpc.WithErrorHandler(func(method string, err error) {
		log.Printf("handler %q error: %v", method, err)
	}))

	rpc.Handle("add", jsonrpc.Typed(func(ctx context.Context, p pair) (any, error) {
		return p.A + p.B, nil
	}))
	rpc.Handle("mul", jsonrpc.Typed(func(ctx context.Context, p pair) (any, error) {
		return p.A * p.B, nil
	}))
	rpc.Handle("div", jsonrpc.Typed(func(ctx context.Context, p pair) (any, error) {
		if p.B == 0 {
			return nil, jsonrpc.NewError(jsonrpc.CodeInvalidParams, "division by zero", map[string]string{"field": "b"})
		}
		return p.A / p.B, nil
	}))

	// The server is an http.Handler; mount it wherever you like.
	http.Handle("/rpc", rpc)
	log.Println("listening on :8080/rpc")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
