// Command stdio-client launches a JSON-RPC stdio server as a subprocess and
// talks to it over the child's stdin/stdout. This is the pattern used to
// consume an MCP-style stdio server.
//
// Run it against the stdio-server example:
//
//	go build -o /tmp/rpc-stdio-server ./examples/stdio-server
//	go run ./examples/stdio-client /tmp/rpc-stdio-server
//
// If no command is given it defaults to running the sibling example via `go run`.
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/paularlott/jsonrpc"
)

func main() {
	name := "go"
	args := []string{"run", "github.com/paularlott/jsonrpc/examples/stdio-server"}
	if len(os.Args) > 1 {
		name = os.Args[1]
		args = os.Args[2:]
	}

	// Spawn the server. Its stderr is inherited so its logs are visible; its
	// stdin/stdout carry the JSON-RPC protocol.
	transport, err := jsonrpc.NewProcessTransport(name, args, jsonrpc.WithStderr(os.Stderr))
	if err != nil {
		log.Fatalf("spawn server: %v", err)
	}
	client := jsonrpc.NewClient(transport)
	defer client.Close()

	ctx := context.Background()

	var greeting string
	if err := client.Call(ctx, "greet", map[string]string{"name": "Ada"}, &greeting); err != nil {
		log.Fatalf("greet: %v", err)
	}
	fmt.Println(greeting)

	var shout string
	if err := client.Call(ctx, "upper", "hello from a subprocess", &shout); err != nil {
		log.Fatalf("upper: %v", err)
	}
	fmt.Println(shout)

	// Fire-and-forget notification: no response is expected.
	if err := client.Notify(ctx, "log", "client is about to disconnect"); err != nil {
		log.Fatalf("notify: %v", err)
	}
}
