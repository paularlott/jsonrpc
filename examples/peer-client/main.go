// Command peer-client is the host side of a bidirectional JSON-RPC 2.0
// connection. It spawns the peer-plugin example as a subprocess, calls its
// "greet" method, and serves the "progress" callback the plugin invokes
// mid-call. The host is itself a Peer: it is the Client for "greet" and the
// Server for "progress".
//
//	go build -o /tmp/peer-plugin ./examples/peer-plugin
//	go run ./examples/peer-client /tmp/peer-plugin
//
// With no arguments it runs the sibling example via `go run`.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/paularlott/jsonrpc"
)

func main() {
	name := "go"
	args := []string{"run", "github.com/paularlott/jsonrpc/examples/peer-plugin"}
	if len(os.Args) > 1 {
		name = os.Args[1]
		args = os.Args[2:]
	}

	// Inbound: the host is the Server for the plugin's callbacks. Register
	// them on a server, then hand it to the peer.
	hostSrv := jsonrpc.NewServer()
	hostSrv.Handle("progress", func(ctx context.Context, params json.RawMessage) (any, error) {
		var msg string
		_ = jsonrpc.Unmarshal(params, &msg)
		fmt.Printf("[callback] plugin: %s\n", msg)
		return "ok", nil // the plugin (acting as client) receives this result
	})

	// Spawn the plugin as a bidirectional peer. NewProcessPeer starts the
	// reader itself, so the returned peer can both call and serve immediately.
	peer, err := jsonrpc.NewProcessPeer(name, args, hostSrv, jsonrpc.WithStderr(os.Stderr))
	if err != nil {
		log.Fatalf("spawn plugin: %v", err)
	}
	defer peer.Close()

	// Outbound: the host is the Client for "greet". While the plugin handles
	// this, it calls "progress" back on us (served by hostSrv above).
	var greeting string
	if err := peer.Client().Call(context.Background(), "greet", "World", &greeting); err != nil {
		log.Fatalf("greet: %v", err)
	}
	fmt.Println(greeting)
}
