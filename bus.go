package relay

import (
	"fmt"
	"reflect"
	"sync"
)

const (
	// busDefaultMaxConcurrentHandlers is the default max number of handlers to run concurrently
	busDefaultMaxConcurrentHandlers = 4
)

var (
	// ErrInvalidHandler indicates that a handler function is invalid
	ErrInvalidHandler = fmt.Errorf("relay: invalid handler function")
	// ErrNoHandlers indicates that there are no handlers for the event type
	ErrNoHandlers = fmt.Errorf("relay: no handlers")
)

// Emitter is an interface for emitting events
type Emitter interface {
	// Emit emits an event where handler functions are invoked sequentially in a single goroutine
	// non-blocking unless the max concurrency limit is reached
	// returns ErrInvalidEvent if the event is invalid
	// returns ErrNoHandlers if no handlers are registered for the event type
	Emit(Event) error

	// EmitConcurrent emits an event concurrently where all handler functions are invoked in
	// their own goroutine
	// non-blocking unless the max concurrency limit is reached
	// returns ErrInvalidEvent if the event is invalid
	// returns ErrNoHandlers if no handlers are registered for the event type
	EmitConcurrent(Event) error

	// EmitSync emits an event synchronously where handler functions are invoked sequentially
	// blocking until all handlers are done
	// returns ErrInvalidEvent if the event is invalid
	// returns ErrNoHandlers if no handlers are registered for the event type
	EmitSync(Event) error
}

// Handler is an interface for handling events
type Handler interface {
	// Handle registers a handler function for the given event type
	// returns ErrInvalidHandler if the handler function is invalid
	// returns ErrInvalidEvent if the event type is invalid
	Handle(Event, HandlerFunc) error
}

// HandlerFunc is a function that processes an event
// event type must be a named struct or pointer to a named struct
// a struct vs a pointer to a struct are considered different types
type HandlerFunc = func(Event)

// NewHandlerFunc creates a Handler for the given function
// the function must accept a single argument of type T
// T must be a named struct or pointer to a named struct
// a struct vs a pointer to a struct are considered different types
// panics if the function is nil or does not match the expected signature
func NewHandlerFunc[T Event](fn func(event T)) (T, HandlerFunc) {
	if fn == nil {
		panic("relay: handler function must not be nil")
	}
	var v T
	return v, func(event Event) {
		e, ok := event.(T)
		if !ok {
			panic(fmt.Sprintf(
				"relay: handler expected event of type '%T', got '%T'", *new(T), event,
			))
		}
		fn(e)
	}
}

// Bus is the interface for an event bus
type Bus interface {
	Emitter
	Handler
}

// BusOptions are the options for an event bus
type BusOptions struct {
	MaxConcurrentHandlers  int  // max number of handlers to run concurrently, defaults to 4
	UseFullyQualifiedNames bool // use fully qualified names for event type keys, defaults to false
}

// setDefaultBusOptions sets the default values for BusOptions
func setDefaultBusOptions(options BusOptions) BusOptions {
	if options.MaxConcurrentHandlers <= 0 {
		options.MaxConcurrentHandlers = busDefaultMaxConcurrentHandlers
	}
	return options
}

// EventBus is an implementation of the Bus interface
type EventBus struct {
	handlers map[string][]HandlerFunc
	mu       sync.RWMutex
	opts     BusOptions
	sem      chan struct{}
}

// NewBus creates a new EventBus with the given configuration
// if no configuration is provided, defaults are used
func NewBus(options ...BusOptions) *EventBus {
	var opts BusOptions
	if len(options) > 0 {
		opts = options[0]
	}
	opts = setDefaultBusOptions(opts)
	return &EventBus{
		handlers: map[string][]HandlerFunc{},
		opts:     opts,
		sem:      make(chan struct{}, opts.MaxConcurrentHandlers),
	}
}

// Cancel removes a previously registered handler
// if the handler is not found, does nothing
func (b *EventBus) Cancel(fn HandlerFunc) {
	id := reflect.ValueOf(fn).Pointer()

	b.mu.Lock()
	defer b.mu.Unlock()

	for k, r := range b.handlers {
		for i, h := range r {
			if reflect.ValueOf(h).Pointer() == id {
				// copy-on-write to preserve immutable handler snapshots for emit methods
				next := make([]HandlerFunc, 0, len(r)-1)
				next = append(next, r[:i]...)
				next = append(next, r[i+1:]...)

				if len(next) == 0 {
					delete(b.handlers, k)
				} else {
					b.handlers[k] = next
				}
				return
			}
		}

	}
}

// Emit emits an event where handler functions are invoked sequentially in a single goroutine
// non-blocking unless the max concurrency limit is reached
// returns ErrInvalidEvent if the event is invalid
// returns ErrNoHandlers if no handlers are registered for the event type
func (b *EventBus) Emit(e Event) error {
	k, err := resolveEventKey(e, b.opts.UseFullyQualifiedNames)
	if err != nil {
		return err
	}

	b.mu.RLock()
	handlers := b.handlers[k]
	b.mu.RUnlock()

	if len(handlers) == 0 {
		return fmt.Errorf("%w: %s", ErrNoHandlers, k)
	}

	b.sem <- struct{}{} // acquire, block if maxConcurrentHandlers reached
	go func() {
		defer func() { <-b.sem }()
		for _, h := range handlers {
			h(e)
		}
	}()
	return nil
}

// EmitConcurrent emits an event concurrently where all handler functions are invoked in
// their own goroutine
// non-blocking unless the max concurrency limit is reached
// returns ErrInvalidEvent if the event is invalid
// returns ErrNoHandlers if no handlers are registered for the event type
func (b *EventBus) EmitConcurrent(e Event) error {
	k, err := resolveEventKey(e, b.opts.UseFullyQualifiedNames)
	if err != nil {
		return err
	}

	b.mu.RLock()
	handlers := b.handlers[k]
	b.mu.RUnlock()

	if len(handlers) == 0 {
		return fmt.Errorf("%w: %s", ErrNoHandlers, k)
	}

	for _, h := range handlers {
		b.sem <- struct{}{} // acquire, block if maxConcurrentHandlers reached

		go func(fn HandlerFunc) {
			defer func() { <-b.sem }()
			fn(e)
		}(h)
	}
	return nil
}

// EmitSync emits an event synchronously where handler functions are invoked sequentially
// blocking until all handlers are done
// returns ErrInvalidEvent if the event is invalid
// returns ErrNoHandlers if no handlers are registered for the event type
func (b *EventBus) EmitSync(e Event) error {
	k, err := resolveEventKey(e, b.opts.UseFullyQualifiedNames)
	if err != nil {
		return err
	}

	b.mu.RLock()
	handlers := b.handlers[k]
	b.mu.RUnlock()

	if len(handlers) == 0 {
		return fmt.Errorf("%w: %s", ErrNoHandlers, k)
	}

	for _, h := range handlers {
		h(e)
	}
	return nil
}

// Handle registers a handler for the given event type
// returns ErrInvalidHandler if the handler function is invalid
// returns ErrInvalidEvent if the event type is invalid
func (b *EventBus) Handle(e Event, fn HandlerFunc) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if fn == nil {
		return fmt.Errorf("%w: handler function must not be nil", ErrInvalidHandler)
	}

	k, err := resolveEventKey(e, b.opts.UseFullyQualifiedNames)
	if err != nil {
		return err
	}

	// copy-on-write to preserve immutable handler snapshots for emit methods
	old := b.handlers[k]
	next := make([]HandlerFunc, len(old)+1)
	copy(next, old)
	next[len(old)] = fn
	b.handlers[k] = next
	return nil
}

// Handlers returns a copy of the map of registered handlers
func (b *EventBus) Handlers() map[string][]HandlerFunc {
	b.mu.RLock()
	defer b.mu.RUnlock()

	m := make(map[string][]HandlerFunc, len(b.handlers))
	for k, v := range b.handlers {
		m[k] = append([]HandlerFunc{}, v...)
	}
	return m
}

// Handle registers a handler func for the given event type on the provided handler
// returns ErrInvalidHandler if the handler function is invalid
// returns ErrInvalidEvent if the event type is invalid
func Handle[T Event](h Handler, fn func(event T)) error {
	if h == nil {
		return fmt.Errorf("%w: handler must not be nil", ErrInvalidHandler)
	}
	e, hf := NewHandlerFunc(fn)
	return h.Handle(e, hf)
}
