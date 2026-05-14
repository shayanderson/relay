package relay

import (
	"sync"
)

// Emitter is an interface for emitting events
type Emitter interface {
	// Emit emits an event where handler functions are invoked sequentially in a single goroutine
	// non-blocking unless the max concurrency limit is reached
	// panics if no handlers are registered for the event type
	Emit(event any)

	// EmitConcurrent emits an event concurrently where all handler functions are invoked in
	// their own goroutine
	// non-blocking unless the max concurrency limit is reached
	// panics if no handlers are registered for the event type
	EmitConcurrent(event any)

	// EmitSync emits an event synchronously where handler functions are invoked sequentially
	// blocking until all handlers are done
	// panics if no handlers are registered for the event type
	EmitSync(event any)
}

// Handler is an interface for handling events
type Handler interface {
	// Handle registers a handler function for the given event type
	// panics if the event type is not a named struct or pointer to a named struct
	// panics if the fn is nil
	Handle(event any, fn HandlerFunc)
}

// Bus is the interface for an event bus
type Bus interface {
	Emitter
	Handler
}

// instance is the singleton default bus instance
var instance *defaultBus

// init initializes the default bus instance with a default config
func init() {
	instance = &defaultBus{}
	SetDefault(New())
}

// defaultBus is a thread-safe wrapper around a the default bus instance
type defaultBus struct {
	bus Bus
	mu  sync.RWMutex
}

// get returns the current default bus instance
func (d *defaultBus) get() Bus {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.bus
}

// set sets the current default bus instance
func (d *defaultBus) set(b Bus) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.bus = b
}

// Default returns the current default bus instance
func Default() Bus {
	b := instance.get()
	if b == nil {
		panic("relay: default bus is not set, use relay.SetDefault")
	}
	return b
}

// Emit emits an event where handler functions are invoked sequentially in a single goroutine
// non-blocking unless the max concurrency limit is reached
// panics if no handlers are registered for the event type
func Emit(event any) {
	Default().Emit(event)
}

// EmitSync emits an event synchronously on the default bus
// blocking until all handlers are done
// panics if no handlers are registered for the event type
func EmitSync(event any) {
	Default().EmitSync(event)
}

// Handle registers a handler func for the given event type on the provided handler
func Handle[T any](h Handler, fn func(event T)) {
	if h == nil {
		panic("relay: handler must not be nil")
	}
	e, f := NewHandlerFunc(fn)
	h.Handle(e, f)
}

// On registers a handler function for the default bus
// panics if the event type is not a named struct or pointer to a named struct
// panics if the fn is nil
func On[T any](fn func(event T)) {
	Default().Handle(NewHandlerFunc(fn))
}

// SetDefault sets the default bus instance
func SetDefault(b Bus) {
	instance.set(b)
}
