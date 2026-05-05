package relay

import (
	"sync"
)

// EventBus is the interface for an event bus
type EventBus interface {
	// Emit emits an event where handlers are invoked sequentially in a single goroutine
	// non-blocking unless the max concurrency limit is reached
	// panics if no handlers are registered for the event type
	Emit(event any)
	// EmitAsync emits an event asynchronously where all handlers are invoked in their own goroutine
	// non-blocking unless the max concurrency limit is reached
	// panics if no handlers are registered for the event type
	EmitAsync(event any)
	// EmitSync emits an event synchronously where handlers are invoked sequentially
	// blocking until all handlers are done
	// panics if no handlers are registered for the event type
	EmitSync(event any)
	// Handle registers a handler for the given event type
	// panics if the event type is not a named struct or pointer to a named struct
	// panics if the handler is nil
	Handle(event any, handler Handler)
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
	bus EventBus
	mu  sync.RWMutex
}

// get returns the current default bus instance
func (d *defaultBus) get() EventBus {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.bus
}

// set sets the current default bus instance
func (d *defaultBus) set(b EventBus) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.bus = b
}

// Default returns the current default bus instance
func Default() EventBus {
	b := instance.get()
	if b == nil {
		panic("relay: default bus is not set, use relay.SetDefault")
	}
	return b
}

// Emit emits an event asynchronously on the default bus
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

// Handle registers a handler for the given event type on the default bus
// panics if the event type is not a named struct or pointer to a named struct
// panics if the handler is nil
func Handle[T any](handler func(event T)) {
	Default().Handle(NewHandler(handler))
}

// On registers a handler for the given event type on the provided bus
func On[T any](bus EventBus, handler func(event T)) {
	if bus == nil {
		panic("relay: bus cannot be nil")
	}
	e, h := NewHandler(handler)
	bus.Handle(e, h)
}

// SetDefault sets the default bus instance
func SetDefault(b EventBus) {
	instance.set(b)
}
