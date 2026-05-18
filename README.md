# relay 📡

**relay** is a lightweight, type-safe [event bus](#event-bus) and [event queue](#event-queue) for Go.

It supports sequential, concurrent, synchronous, and queued event dispatch with automatic type checking and configurable concurrency limits.

## Features

- **Type-safe handlers** — compile-time safety via generics
- **Concurrent event dispatch** with configurable limits
- **In-memory event queue** with buffered async dispatch
- **Fully qualified type keys** (optional) for avoiding type name collisions
- **Synchronous or concurrent emit** control
- **Zero dependencies** — pure Go implementation with no external dependencies

## Installation

```bash
go get github.com/shayanderson/relay
```

## Event Bus

### Example

```go
bus := relay.NewBus(relay.BusOptions{ // options are optional
    // max number of handlers to run concurrently, defaults to 4
	MaxConcurrentHandlers:  32,
    // use fully qualified names for event type keys to avoid collisions in large projects
	UseFullyQualifiedNames: true,
})

type UserCreated struct{ Name string }

// register a handler for UserCreated event
err := relay.Handle(bus, func(e UserCreated) {
    fmt.Println("new user:", e.Name)
})
// or, can also do:
// bus.Handle(relay.NewHandlerFunc(func(e UserCreated) {
// 	fmt.Println("new user:", e.Name)
// }))
if err != nil {
    // ...
}

// emit a UserCreated event, must be named struct or pointer to named struct
err := bus.Emit(UserCreated{Name: "Alice"})
if err != nil {
    // ...
}
// output:
// new user: Alice

// allow concurrent handlers to finish before exit
time.Sleep(10 * time.Millisecond)
```

#### Context Example

If you need to pass context to your handlers, you can use an event struct that includes a context field:

```go
// event with context
type testEventCtx struct { ctx context.Context }

// create a new bus
b := relay.NewBus()
ctx, cancel := context.WithCancel(context.Background())
wg := sync.WaitGroup{} // to wait for handlers to finish

// register handler that respects context cancellation
err := relay.Handle(b, func(e testEventCtx) {
    defer wg.Done()
    <-e.ctx.Done()
})
if err != nil {
    // ...
}

// emit 3 events
wg.Add(3)
_ = b.Emit(testEventCtx{ctx: ctx})
_ = b.Emit(testEventCtx{ctx: ctx})
_ = b.Emit(testEventCtx{ctx: ctx})
cancel() // cancel context to unblock handlers
wg.Wait() // wait for all handlers to finish
```

### API Overview

#### Event Types

Events must be defined as named struct types or pointers to named struct types.

```go
type MyEvent struct{}
```

#### Type Definitions

```go
type Event any

type HandlerFunc = func(Event)

type BusOptions struct {
    MaxConcurrentHandlers  int
    UseFullyQualifiedNames bool
}

type Emitter interface {
	Emit(Event) error
	EmitConcurrent(Event) error
	EmitSync(Event) error
}

type Handler interface {
	Handle(Event, HandlerFunc) error
}

type Bus interface {
	Emitter
	Handler
}
```

Additional methods available on concrete Bus implementation:

```go
type EventBus struct {}
func (*EventBus) Cancel(HandlerFunc)
func (*EventBus) Handlers() map[string][]HandlerFunc
```

### Functions

- `Handle[T Event](h Handler, fn func(event T)) error`: Registers a handler for type `T` on the provided bus.
  - Handlers for type `T` are different from handlers for type `*T`. A separate handler must be registered for each if using both.
- `NewBus(options ...BusOptions) *EventBus`: Creates a new bus instance with the given options.
- `NewHandlerFunc[T Event](fn func(event T)) (T, HandlerFunc)`: Helper function that returns the zero-value event type `T` and a wrapped handler function.

### Methods

- `Cancel(fn HandlerFunc)`: Cancels a previously registered handler.
- `Emit(e Event) error`: Emits an event, invoking handlers sequentially in a single goroutine.
  - Event must be a named struct or pointer to a named struct.
  - Non-blocking, unless the max concurrency limit is reached, in which case it will block until a handler can be started.
- `EmitConcurrent(e Event) error`: Emits an event on the bus, invoking all handlers concurrently in separate goroutines.
  - Event must be a named struct or pointer to a named struct.
  - Non-blocking, unless the max concurrency limit is reached, in which case it will block until a handler can be started.
- `EmitSync(e Event) error`: Emits an event on the bus synchronously, handlers are invoked sequentially.
  - Event must be a named struct or pointer to a named struct.
  - Blocks until all handlers for the event have completed.
- `Handle(e Event, fn HandlerFunc) error`: Registers a handler for the specified event type.
- `Handlers() map[string][]HandlerFunc`: Returns a map of registered handlers.

### Options

When creating a new bus, you can customize its behavior using `BusOptions`.

- `MaxConcurrentHandlers`: Limits the number of event handlers that can run concurrently. Limit is for each bus instance. Default is `4`.
- `UseFullyQualifiedNames`: If set to `true`, event type keys will include the package path, reducing the risk of type name collisions, e.g. `github.com/you/pkg.UserCreated` instead of just `pkg.UserCreated`. Default is `false`.

## Event Queue

### Example

```go
queue := relay.NewQueue(relay.QueueOptions{ // options are optional
    // size of queue buffer for events, defaults to 128
    BufferSize:  1_000,
    // use fully qualified names for event type keys to avoid collisions in large projects
    UseFullyQualifiedNames: true,
})

// register a subscriber for MessageEvent
err := relay.Subscribe(queue, func(e MessageEvent) {
    fmt.Println("received:", e.Text)
})
// or, can also do:
// queue.Subscribe(relay.NewSubscriberFunc(func(e MessageEvent) {
// 	fmt.Println("received:", e.Text)
// }))
if err != nil {
    // ...
}

ctx := context.Background()

go func() {
    err := queue.Run(ctx)
    if err != nil {
        // ...
    }
}()

err := queue.Publish(MessageEvent{Text: "hello queue"})
if err != nil {
    // ...
}

// output:
// received: hello queue

err := queue.Close()
if err != nil {
    // ...
}
```

### API Overview

#### Event Types

Events must be defined as named struct types or pointers to named struct types.

```go
type MyEvent struct{}
```

#### Type Definitions

```go
type Event any

type SubscriberFunc = func(Event)

type QueueOptions struct {
    BufferSize             int
    UseFullyQualifiedNames bool
}

type Publisher interface {
	Publish(Event) error
}

type Subscriber interface {
	Subscribe(Event, SubscriberFunc) error
}

type Queue interface {
	Publisher
	Subscriber
}
```

Additional methods available on concrete Queue implementation:

```go
type EventQueue struct {}
func (*EventQueue) Close() error
func (*EventQueue) Run(context.Context) error
func (*EventQueue) Unsubscribe(Event, SubscriberFunc) error
```

### Functions

- `NewQueue(options ...QueueOptions) *EventQueue`: Creates a new queue instance with the given options.
- `NewSubscriberFunc[T Event](fn func(event T)) (T, SubscriberFunc)`: Helper function that returns the zero-value event type `T` and a wrapped subscriber function.
- `Subscribe[T Event](s Subscriber, fn func(event T)) error`: Registers a subscriber for type `T` on the provided queue.
  - Subscribers for type `T` are different from subscribers for type `*T`. A separate subscriber must be registered for each if using both.

### Methods

- `Close() error`: Closes the queue. After closing, new events cannot be published, but buffered events may still be delivered.
- `Publish(e Event) error`: Publishes an event to the queue.
  - Event must be a named struct or pointer to a named struct.
- `Run(ctx context.Context) error`: Starts the event processing loop. Should be run in a separate goroutine. Blocks until the context is canceled or the queue is closed.
- `Subscribe(e Event, fn SubscriberFunc) error`: Registers a subscriber for the specified event type. Returns an error if the subscriber function is invalid or if the event type is invalid.
- `Unsubscribe(e Event, fn SubscriberFunc) error`: Unregisters a previously registered subscriber.

### Options

When creating a new queue, you can customize its behavior using `QueueOptions`.

- `BufferSize`: The size of the internal channel buffer for events. Larger buffers can help prevent queue is full errors when publishing events, but will consume more memory. Default is `128`.
- `UseFullyQualifiedNames`: If set to `true`, event type keys will include the package path, reducing the risk of type name collisions, e.g. `github.com/you/pkg.UserCreated` instead of just `pkg.UserCreated`. Default is `false`.

## Tests

Tests can be run with:

```bash
make test
```

Benchmarks can be run with:

```bash
make test-bench
```
