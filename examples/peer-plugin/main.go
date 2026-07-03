// Command peer-plugin is the server side of a bidirectional JSON-RPC 2.0
// connection. It serves the "greet" method, and while handling it calls
// "progress" back on its caller — the same stdin/stdout stream carries both
// directions. That is what makes it a Peer rather than a plain server.
//
// It is driven by the peer-client example; you would not normally run it
// directly.
package main

import (
	"context"
	"encoding/json"
	"log"
	"os"

	"github.com/paularlott/jsonrpc"
)

func main() {
	// Protocol frames go to stdout; anything else would corrupt the stream, so
	// logs go to stderr.
	log.SetOutput(os.Stderr)

	srv := jsonrpc.NewServer()
	// A Peer is a Server (inbound) plus a Client (outbound) over one stream.
	// Build it from the server; the outbound client is peer.Client() below.
	peer := jsonrpc.NewPeer(os.Stdin, os.Stdout, srv)

	// Inbound: the plugin is the Server for "greet".
	srv.Handle("greet", func(ctx context.Context, params json.RawMessage) (any, error) {
		var name string
		if err := jsonrpc.Unmarshal(params, &name); err != nil {
			return nil, err
		}
		// Outbound: now the plugin is the Client, calling a method the host
		// serves. This reverse-direction call rides the same connection.
		var ack string
		if err := peer.Client().Call(ctx, "progress", "about to greet "+name, &ack); err != nil {
			return nil, err
		}
		log.Printf("host acknowledged progress: %s", ack)
		return "Hello, " + name + "!", nil
	})

	// NewPeer does not start the reader; Serve runs it until stdin closes
	// (the host disconnected). Handlers may be registered any time before Serve.
	if err := peer.Serve(); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
