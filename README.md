# jsonrpc

A small, transport-agnostic [JSON-RPC 2.0](https://www.jsonrpc.org/specification) library for Go.

The core is a `Server` that dispatches decoded requests to registered handlers.
The same dispatch logic backs every transport, so an identical set of methods
can be served over HTTP and over a byte stream (stdin/stdout) without change. A
matching `Client` talks to a remote endpoint over HTTP, an arbitrary stream, or
a spawned subprocess.

- Full JSON-RPC 2.0: requests, notifications, batches, and the standard error codes.
- Request ids round-trip byte-for-byte (number, string, or null shape preserved).
- Params are handed to handlers as raw JSON, so integer precision and custom
  decoding are entirely under your control.
- Concurrent dispatch by default, with serialised writes.
- No third-party dependencies.

```
go get github.com/paularlott/jsonrpc
```

Requires the Go version declared in [`go.mod`](go.mod). The typed-handler helper
uses generics (Go 1.18+).

## Server

A `Server` is a method registry. Register handlers, then serve them over any
transport.

```go
srv := jsonrpc.NewServer()

// Raw handler: decode params yourself.
srv.Handle("echo", func(ctx context.Context, params json.RawMessage) (any, error) {
    var v any
    if err := jsonrpc.Unmarshal(params, &v); err != nil {
        return nil, err // becomes an Invalid Params error
    }
    return v, nil
})

// Typed handler: params decoded into T for you.
srv.Handle("add", jsonrpc.Typed(func(ctx context.Context, p struct {
    A, B int
}) (any, error) {
    return p.A + p.B, nil
}))
```

### Over HTTP

`Server` implements `http.Handler`. Mount it anywhere:

```go
http.Handle("/rpc", srv)
http.ListenAndServe(":8080", nil)
```

It accepts `POST` with a single request object or a batch array. A message that
produces no response (a notification, or an all-notification batch) returns
`204 No Content`; otherwise the JSON response is returned with `200 OK`.

### Over a stream (stdio)

`ServeStream` runs a newline-delimited JSON-RPC loop, the shape used for stdio
servers (including MCP stdio servers):

```go
srv.ServeStream(context.Background(), os.Stdin, os.Stdout)
```

It reads JSON values from the reader (they need not be newline-separated) and
writes each response as one line to the writer. It blocks until the reader
reaches EOF. Keep stdout clean — write logs to stderr.

### Embedding in another transport

`HandleMessage(ctx, raw) ([]byte, bool)` dispatches one raw message (single or
batch) and returns the marshalled response and whether a response is due.
`HandleRequest(ctx, *Request) *Response` dispatches a single decoded request
(returning `nil` for notifications). Use these to wire the dispatcher into a
transport the library doesn't provide.

## Client

A `Client` wraps a `Transport`, generating ids and encoding params for you.

```go
client := jsonrpc.NewClient(jsonrpc.NewHTTPTransport("http://localhost:8080/rpc"))
defer client.Close()

var sum int
err := client.Call(ctx, "add", map[string]int{"A": 2, "B": 40}, &sum) // sum == 42

// Notifications expect no response.
err = client.Notify(ctx, "ping", nil)
```

A JSON-RPC error response is returned as a `*jsonrpc.Error`:

```go
err := client.Call(ctx, "div", pair{A: 1, B: 0}, nil)
var rpcErr *jsonrpc.Error
if errors.As(err, &rpcErr) {
    fmt.Println(rpcErr.Code, rpcErr.Message, rpcErr.Data)
}
```

### Batching

`CallBatch` sends several calls as one JSON-RPC 2.0 batch request instead of
one round-trip per call:

```go
var sum, product int
results := client.CallBatch(ctx, []jsonrpc.BatchCall{
    {Method: "add", Params: pair{A: 2, B: 40}, Out: &sum},
    {Method: "mul", Params: pair{A: 6, B: 7}, Out: &product},
})
for i, r := range results {
    if r.Err != nil {
        fmt.Println("call", i, "failed:", r.Err)
    }
}
```

Results are always returned in call order, regardless of transport or response
ordering, and one call's error (transport failure or JSON-RPC error response,
returned as `*jsonrpc.Error`) does not affect the others. `HTTPTransport` and
`StreamTransport` both send the batch as a single wire message (one HTTP POST,
or one JSON array line); other `Transport` implementations still work via a
concurrent per-call fallback, so `CallBatch` is always safe to use.

### Transports

| Transport  | Constructor                            | Use                                                                                                                           |
| ---------- | -------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| HTTP       | `NewHTTPTransport(url, ...)`           | Talk to an HTTP JSON-RPC endpoint. Options: `WithHTTPClient`, `WithHeader`.                                                   |
| Stream     | `NewStreamTransport(in, out, ...)`     | Newline-delimited JSON over any reader/writer (pipes, sockets).                                                               |
| Subprocess | `NewProcessTransport(name, args, ...)` | Spawn a command and speak JSON-RPC over its stdin/stdout. Options: `WithStderr`, `WithShutdownTimeout`, `WithEnv`, `WithExtraEnv`, `WithDir`, `WithOnExit`. |

The stream and subprocess transports run a background reader that correlates
responses to calls by id, so many calls and notifications can be in flight
concurrently.

### Subprocess lifecycle

`NewProcessTransport` reaps the child in a background goroutine the moment it
exits — whether it crashes mid-session or is shut down by `Close` — so it never
becomes a zombie. Pass `WithOnExit` to be told when that happens:

```go
transport, err := jsonrpc.NewProcessTransport(serverCmd, args,
    jsonrpc.WithOnExit(func(err error) {
        log.Printf("server exited: %v", err) // nil for a clean exit
        // unregister, restart, etc.
    }),
)
```

The callback fires exactly once, asynchronously, after the child has been
reaped; it must not block, and `Close` does not wait for it. Without it a caller
only learns a spawned server has died when subsequent calls start failing.

`WithEnv` and `WithExtraEnv` control the child's environment. `WithEnv` **replaces**
the whole environment (os/exec `Cmd.Env` semantics — every variable, including
`PATH`/`HOME`, must be listed). `WithExtraEnv` **merges** `KEY=VALUE` entries on
top of the inherited parent environment, so it is the right choice for setting a
few variables while keeping the rest intact; later values win for a repeated key.

```go
// Keep the parent environment; add/override a couple of variables.
transport, _ := jsonrpc.NewProcessTransport(serverCmd, args,
    jsonrpc.WithExtraEnv("LOG_LEVEL=debug", "DATA_DIR=/var/data"),
)
```

## Peer (bidirectional)

A [`Client`](#client) only sends requests; a [`Server`](#server) only handles
them. A `Peer` is **a Client and a Server sharing one connection**, so each end
can both call out and serve calls in. Use it when the two sides talk back and
forth — for example a host that calls a plugin's methods while the plugin calls
host "callback" methods back (remote objects, progress, elicitation). **Both
ends are Peers**: whoever initiates a call is acting as the Client for that
call, and whoever receives it is acting as the Server.

The two ends connect with a bidirectional stream — for a subprocess that is the
child's stdin/stdout; in-process it would be a pair of `io.Pipe`s or a socket.
Each end is a Peer: whoever initiates a call is acting as the Client, whoever
receives it is acting as the Server. The runnable programs live in
`examples/peer-plugin` and `examples/peer-client`; both are shown in full below.

### Plugin example

`examples/peer-plugin` serves `greet` and, while handling it, calls `progress`
back on the host. It is a Peer over stdin/stdout: `NewPeer` builds it from its
inbound server, and `peer.Client()` is its outbound side. Construction order
matters — `NewPeer` does **not** start the reader, so the `greet` handler is
registered after the peer is created and before `Serve` runs it (the handler
closes over `peer`, which is assigned by then).

```go
// Command peer-plugin is the server side of a bidirectional JSON-RPC 2.0
// connection. It serves "greet" and, mid-call, calls "progress" back on its
// caller — the same stdin/stdout stream carries both directions.
package main

import (
	"context"
	"encoding/json"
	"log"
	"os"

	"github.com/paularlott/jsonrpc"
)

func main() {
	log.SetOutput(os.Stderr) // stdout is the protocol stream; logs go to stderr

	srv := jsonrpc.NewServer()
	// A Peer = a Server (inbound) + a Client (outbound) over one stream.
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

	if err := peer.Serve(); err != nil { // blocks until stdin closes
		log.Fatalf("serve: %v", err)
	}
}
```

### Client example

`examples/peer-client` spawns the plugin and is itself a Peer: it is the Client
for `greet` and the Server for the `progress` callback. The packages used are
just `context`, `encoding/json`, and `jsonrpc`. Setup is: register the callback
handler on a server, then `NewProcessPeer` spawns the plugin with that server
(and starts the reader automatically). The call to the plugin and the callback
it triggers share the one connection.

```go
// Command peer-client spawns peer-plugin, calls its "greet" method, and serves
// the "progress" callback the plugin invokes mid-call.
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

	// Inbound: the host is the Server for the plugin's callbacks.
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
```

Build the plugin and run the client against it:

```
go build -o /tmp/peer-plugin ./examples/peer-plugin
go run ./examples/peer-client /tmp/peer-plugin
```

The callback fires mid-`greet` (the plugin is acting as a client to the host),
then the greeting is returned:

```
[callback] plugin: about to greet World
host acknowledged progress: ok
Hello, World!
```

How it fits together:

- `peer.Client()` is the outbound side — a real `*Client` (generates ids,
  correlates responses). `peer.Handle(...)` is the inbound side — it delegates
  to the `*Server` passed to `NewPeer`.
- The reader demultiplexes each incoming message: a **response** (no `method`)
  goes to the pending outbound `Client.Call`; a **request** (has `method`) is
  dispatched to the `Server`, and its response is written back. So the same
  bytes carry both directions, classified by the presence of `method`.
- Inbound handlers run in their own goroutine, so a slow handler — or one that
  makes a nested reverse-direction call (like `do_work` calling `progress`)
  cannot stall delivery of outbound responses.
- `NewPeer` does **not** start the reader; call `Serve` from the goroutine that
  owns the input. That lets a caller wire up state its handlers depend on before
  any handler can run. `Serve` blocks until EOF; follow it with `peer.Wait()` to
  block until in-flight handlers have flushed. `Peer.Close` cancels in-flight
  handler contexts and runs a close function given via `WithPeerCloseFunc`.

For a spawned subprocess peer, use `NewProcessPeer(name, args, server, opts...)`
— the bidirectional counterpart to `NewProcessTransport`. It accepts the same
process options (`WithStderr`, `WithEnv`, `WithExtraEnv`, `WithDir`,
`WithOnExit`, `WithShutdownTimeout`) and closing the Peer drives the child down. Unlike
`NewPeer`, `NewProcessPeer` starts the background reader itself, so the returned
Peer is ready to call immediately.

## Errors

Return a `*jsonrpc.Error` from a handler to control the wire error exactly; any
other non-nil error becomes a server error (`-32000`). A handler panic is
recovered and reported as an internal error (`-32603`).

```go
return nil, jsonrpc.NewError(jsonrpc.CodeInvalidParams, "division by zero", map[string]string{"field": "b"})
```

Standard codes are exported as `CodeParseError`, `CodeInvalidRequest`,
`CodeMethodNotFound`, `CodeInvalidParams`, `CodeInternalError` and
`CodeServerError`, with matching constructor helpers (`ParseError`,
`InvalidRequest`, …).

## Notifications

A notification is a request without an id; the same handler serves both request
and notification calls, and the return value is discarded for notifications. An
unknown notification is silently ignored (per spec), while an unknown request
returns a Method Not Found error. To observe errors from notification handlers,
pass `WithErrorHandler`.

Call `jsonrpc.IsNotification(ctx)` inside a handler to tell the two call shapes
apart — for example to reject a method-only handler when it's invoked as a
notification, or vice versa:

```go
srv.Handle("event", func(ctx context.Context, params json.RawMessage) (any, error) {
    if !jsonrpc.IsNotification(ctx) {
        return nil, jsonrpc.MethodNotFound("event is notification-only")
    }
    // ... handle the fire-and-forget call
    return nil, nil
})
```

## Concurrency

By default the server dispatches each incoming message — and each element of a
batch — concurrently, serialising only the writes. Pass `WithSerialDispatch` to
process messages one at a time in arrival order. The library is verified with
`go test -race` and a fuzz target over `HandleMessage`.

## Examples

Runnable programs live in [`examples/`](examples/):

- [`http-server`](examples/http-server) / [`http-client`](examples/http-client) — a calculator over HTTP.
- [`stdio-server`](examples/stdio-server) / [`stdio-client`](examples/stdio-client) — a stdio server driven by a client that launches it as a subprocess.
- [`peer-plugin`](examples/peer-plugin) / [`peer-client`](examples/peer-client) — a bidirectional peer: a plugin that calls a host callback mid-request, and the client that serves it.

## License

MIT — see [LICENSE.txt](LICENSE.txt).
