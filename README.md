# relay 📡

**relay** is a lightweight, type-safe, concurrent event bus for Go.

It allows you to register event handlers by type and emit events asynchronously (or synchronously) with automatic type checking and configurable concurrency limits.

## Features

- **Type-safe handlers** — compile-time safety via generics
- **Concurrent event dispatch** with configurable limits
- **Global default bus** for convenience
- **Fully qualified type keys** (optional) for avoiding type name collisions
- **Synchronous or asynchronous emit** control
- **Zero dependencies** — pure Go implementation with no external dependencies

## Installation

```bash
go get github.com/shayanderson/relay
```

## Usage

### Basic Example

```go
package main

import (
	"fmt"
	"time"

	"github.com/shayanderson/relay"
)

// define an event type, always a struct
type MessageEvent struct {
	Text string
}

func main() {
	// register a handler for MessageEvent
	relay.Handle(func(e MessageEvent) {
		fmt.Println("received:", e.Text)
	})

	// emit an event, must be named struct or pointer to named struct
	relay.Emit(MessageEvent{Text: "hello relay"})

	// allow async handlers to finish before exit
	time.Sleep(10 * time.Millisecond)
}
// output:
// received: hello relay
```

### Custom Bus Example

You can create your own bus instance instead of using the global default:

```go
bus := relay.New(relay.Config{ // config is optional
    // max number of handlers to run concurrently, defaults to 16
	MaxConcurrentHandlers:  32,
    // use fully qualified names for event type keys, to avoid collisions in large projects
	UseFullyQualifiedNames: true,
})

type UserCreated struct{ Name string }

bus.Handle(relay.NewHandler(func(e UserCreated) {
	fmt.Println("new user:", e.Name)
}))
bus.Emit(UserCreated{Name: "Alice"})
// output:
// new user: Alice
```

### Context Example

If you need to pass context to your handlers, you can use a event struct that includes a context field:

```go
// event with context
type testEventCtx struct { ctx context.Context }

// create a new bus
b := New()
ctx, cancel := context.WithCancel(context.Background())
wg := sync.WaitGroup{} // to wait for handlers to finish

// register handler that respects context cancellation
b.Handle(NewHandler(func(e testEventCtx) {
    defer wg.Done()
    <-e.ctx.Done()
}))

// emit 3 events
wg.Add(3)
b.Emit(testEventCtx{ctx: ctx})
b.Emit(testEventCtx{ctx: ctx})
b.Emit(testEventCtx{ctx: ctx})
cancel() // cancel context to unblock handlers
wg.Wait() // wait for all handlers to finish
```

## Configuration

When creating a new bus, you can customize its behavior using `relay.Config`.

- `MaxConcurrentHandlers`: Limits the number of event handlers that can run concurrently. Limit is for each bus instance. Default is `16`.
- `UseFullyQualifiedNames`: If set to `true`, event type keys will include the package path, reducing the risk of type name collisions, e.g. `github.com/you/pkg.UserCreated` instead of just `pkg.UserCreated`. Default is `false`.

## API Overview

### Type Definitions

```go
type Handler func(event any)

type Config struct {
    MaxConcurrentHandlers  int
    UseFullyQualifiedNames bool
}

type EventBus interface {
    Emit(event any)
	EmitAsync(event any)
    EmitSync(event any)
    Handle(event any, handler Handler)
}
```

### Functions

- `relay.New(config ...Config) *Bus`: Creates a new bus instance with the given configuration.
- `relay.Default() EventBus`: Returns the current default bus instance.
- `relay.Emit(event any)`: Emit emits an event on the default bus, where handlers are invoked sequentially in a single goroutine.
  - `event` must be a named struct or pointer to a named struct.
  - Non-blocking, unless the max concurrency limit is reached, in which case it will block until a handler can be started.
- `relay.EmitAsync(event any)`: EmitAsync emits an event on the default bus, asynchronously where all handlers are invoked in their own goroutine.
  - `event` must be a named struct or pointer to a named struct.
  - Non-blocking, unless the max concurrency limit is reached, in which case it will block until a handler can be started.
- `relay.EmitSync(event any)`: EmitSync emits an event on the default bus synchronously where handlers are invoked sequentially.

  - `event` must be a named struct or pointer to a named struct.
  - Blocks until all handlers for the event have completed.

- `relay.Handle[T any](fn func(event T))`: Registers a handler for type `T` on the default bus.
  - `T` must be a named struct or pointer to a named struct.
  - Handlers for type `T` are different from handlers for type `*T`. A separate handler must be registered for each if using both.
- `relay.SetDefault(bus EventBus)`: Sets the default bus instance.

### Bus Methods

- `Cancel(handler Handler)`: Cancels a previously registered handler.
- `Emit(event any)`: Emits an event where handlers are invoked sequentially in a single goroutine.
  - `event` must be a named struct or pointer to a named struct.
  - Non-blocking, unless the max concurrency limit is reached, in which case it will block until a handler can be started.
- `EmitAsync(event any)`: Emits an event asynchronously where all handlers are invoked in their own goroutine.
  - `event` must be a named struct or pointer to a named struct.
  - Non-blocking, unless the max concurrency limit is reached, in which case it will block until a handler can be started.
- `EmitSync(event any)`: Emits an event synchronously where handlers are invoked sequentially.
  - `event` must be a named struct or pointer to a named struct.
  - Blocks until all handlers for the event have completed.
- `Handle(event any, handler Handler)`: Registers a handler for the specified event type.
- `Handlers() map[string][]Handler`: Returns a map of registered handlers.

## Testing

Tests can be run with:

```bash
make test
```

Benchmarks can be run with:

```bash
make test-bench
```
