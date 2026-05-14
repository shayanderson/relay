# relay 📡

**relay** is a lightweight, type-safe event bus with concurrent handler execution for Go.

It allows you to register event handlers by type and supports sequential, concurrent, and synchronous event dispatch with automatic type checking and configurable concurrency limits.

## Features

- **Type-safe handlers** — compile-time safety via generics
- **Concurrent event dispatch** with configurable limits
- **Global default bus** for convenience
- **Fully qualified type keys** (optional) for avoiding type name collisions
- **Synchronous or concurrent emit** control
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
	relay.On(func(e MessageEvent) {
		fmt.Println("received:", e.Text)
	})

	// emit an event, must be named struct or pointer to named struct
	relay.Emit(MessageEvent{Text: "hello relay"})

	// allow concurrent handlers to finish before exit
	time.Sleep(10 * time.Millisecond)
}
// output:
// received: hello relay
```

### Custom Bus Example

You can create your own bus instance instead of using the global default:

```go
bus := relay.New(relay.Config{ // config is optional
    // max number of handlers to run concurrently, defaults to 4
	MaxConcurrentHandlers:  32,
    // use fully qualified names for event type keys to avoid collisions in large projects
	UseFullyQualifiedNames: true,
})

type UserCreated struct{ Name string }

relay.Handle(bus, func(e UserCreated) {
    fmt.Println("new user:", e.Name)
})
// or, can also:
// bus.Handle(relay.NewHandlerFunc(func(e UserCreated) {
// 	fmt.Println("new user:", e.Name)
// }))

bus.Emit(UserCreated{Name: "Alice"})
// output:
// new user: Alice
```

### Context Example

If you need to pass context to your handlers, you can use an event struct that includes a context field:

```go
// event with context
type testEventCtx struct { ctx context.Context }

// create a new bus
b := relay.New()
ctx, cancel := context.WithCancel(context.Background())
wg := sync.WaitGroup{} // to wait for handlers to finish

// register handler that respects context cancellation
relay.Handle(b, func(e testEventCtx) {
    defer wg.Done()
    <-e.ctx.Done()
})

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

- `MaxConcurrentHandlers`: Limits the number of event handlers that can run concurrently. Limit is for each bus instance. Default is `4`.
- `UseFullyQualifiedNames`: If set to `true`, event type keys will include the package path, reducing the risk of type name collisions, e.g. `github.com/you/pkg.UserCreated` instead of just `pkg.UserCreated`. Default is `false`.

## API Overview

### Event Types

Events must be defined as named struct types or pointers to named struct types.

```go
type MyEvent struct{}
```

### Type Definitions

```go
type HandlerFunc func(event any)

type Config struct {
    MaxConcurrentHandlers  int
    UseFullyQualifiedNames bool
}

type Emitter interface {
	Emit(event any)
	EmitConcurrent(event any)
	EmitSync(event any)
}

type Handler interface {
	Handle(event any, fn HandlerFunc)
}

type Bus interface {
	Emitter
	Handler
}
```

Additional methods available on concrete Bus implementation:

```go
type EventBus struct {
    // unexported fields
}

func (*EventBus) Cancel(HandlerFunc)
func (*EventBus) Handlers() map[string][]HandlerFunc
```

### Functions

- `relay.New(config ...Config) *EventBus`: Creates a new bus instance with the given configuration.
- `relay.Default() Bus`: Returns the current default bus instance.
- `relay.Emit(event any)`: Emits an event on the default bus, invoking handlers sequentially in a single goroutine.
  - `event` must be a named struct or pointer to a named struct.
  - Non-blocking, unless the max concurrency limit is reached, in which case it will block until a handler can be started.
- `relay.EmitConcurrent(event any)`: Emits an event on the default bus, invoking all handlers concurrently in separate goroutines.
  - `event` must be a named struct or pointer to a named struct.
  - Non-blocking, unless the max concurrency limit is reached, in which case it will block until a handler can be started.
- `relay.EmitSync(event any)`: Emits an event on the default bus synchronously, handlers are invoked sequentially.
  - `event` must be a named struct or pointer to a named struct.
  - Blocks until all handlers for the event have completed.
- `relay.Handle[T any](h Handler, fn func(event T))`: Registers a handler for type `T` on the provided bus.
  - Handlers for type `T` are different from handlers for type `*T`. A separate handler must be registered for each if using both.
- `relay.On[T any](fn func(event T))`: Registers a handler for type `T` on the default bus.
  - Handlers for type `T` are different from handlers for type `*T`. A separate handler must be registered for each if using both.
- `relay.SetDefault(bus Bus)`: Sets the default bus instance.

### Bus Methods

- `Cancel(fn HandlerFunc)`: Cancels a previously registered handler.
- `Emit(event any)`: Emits an event, invoking handlers sequentially in a single goroutine.
  - `event` must be a named struct or pointer to a named struct.
  - Non-blocking, unless the max concurrency limit is reached, in which case it will block until a handler can be started.
- `EmitConcurrent(event any)`: Emits an event on the bus, invoking all handlers concurrently in separate goroutines.
  - `event` must be a named struct or pointer to a named struct.
  - Non-blocking, unless the max concurrency limit is reached, in which case it will block until a handler can be started.
- `EmitSync(event any)`: Emits an event on the bus synchronously, handlers are invoked sequentially.
  - `event` must be a named struct or pointer to a named struct.
  - Blocks until all handlers for the event have completed.
- `Handle(event any, fn HandlerFunc)`: Registers a handler for the specified event type.
- `Handlers() map[string][]HandlerFunc`: Returns a map of registered handlers.

## Testing

Tests can be run with:

```bash
make test
```

Benchmarks can be run with:

```bash
make test-bench
```
