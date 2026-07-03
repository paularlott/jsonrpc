// Command http-client calls the http-server example.
//
// Start the server first (go run ./examples/http-server), then:
//
//	go run ./examples/http-client
//	go run ./examples/http-client http://localhost:8080/rpc
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/paularlott/jsonrpc"
)

type pair struct {
	A float64 `json:"a"`
	B float64 `json:"b"`
}

func main() {
	url := "http://localhost:8080/rpc"
	if len(os.Args) > 1 {
		url = os.Args[1]
	}

	client := jsonrpc.NewClient(jsonrpc.NewHTTPTransport(url,
		jsonrpc.WithHeader("Authorization", "Bearer example-token"),
	))
	defer client.Close()

	ctx := context.Background()

	var sum float64
	if err := client.Call(ctx, "add", pair{A: 2, B: 40}, &sum); err != nil {
		log.Fatalf("add: %v", err)
	}
	fmt.Printf("add(2, 40) = %v\n", sum)

	var product float64
	if err := client.Call(ctx, "mul", pair{A: 6, B: 7}, &product); err != nil {
		log.Fatalf("mul: %v", err)
	}
	fmt.Printf("mul(6, 7) = %v\n", product)

	// A call that returns a JSON-RPC error is surfaced as *jsonrpc.Error.
	err := client.Call(ctx, "div", pair{A: 1, B: 0}, nil)
	var rpcErr *jsonrpc.Error
	if errors.As(err, &rpcErr) {
		fmt.Printf("div(1, 0) failed as expected: code=%d message=%q data=%v\n",
			rpcErr.Code, rpcErr.Message, rpcErr.Data)
	}

	// CallBatch sends every call in one JSON-RPC batch request (one HTTP POST)
	// instead of one round-trip per call. Results come back in call order.
	var batchSum, batchProduct float64
	results := client.CallBatch(ctx, []jsonrpc.BatchCall{
		{Method: "add", Params: pair{A: 1, B: 1}, Out: &batchSum},
		{Method: "mul", Params: pair{A: 3, B: 3}, Out: &batchProduct},
		{Method: "div", Params: pair{A: 1, B: 0}},
	})
	fmt.Printf("batch: sum=%v product=%v call[2].err=%v\n", batchSum, batchProduct, results[2].Err)
}
