package relay

import (
	"fmt"
	"reflect"
	"sync"
)

// HandlerFunc is a function that processes an event
// event type must be a named struct or pointer to a named struct
// a struct vs a pointer to a struct are considered different types
type HandlerFunc func(event any)

// NewHandlerFunc creates a Handler for the given function
// the function must accept a single argument of type T
// T must be a named struct or pointer to a named struct
// a struct vs a pointer to a struct are considered different types
// panics if the function does not match the expected signature
func NewHandlerFunc[T any](fn func(event T)) (T, HandlerFunc) {
	if fn == nil {
		panic("relay: handler function must not be nil")
	}
	var v T
	return v, func(event any) {
		e, ok := event.(T)
		if !ok {
			panic(fmt.Sprintf(
				"relay: handler expected event of type '%T', got '%T'", *new(T), event,
			))
		}
		fn(e)
	}
}

// Config is the configuration for a bus
type Config struct {
	MaxConcurrentHandlers  int  // max number of handlers to run concurrently, defaults to 4
	UseFullyQualifiedNames bool // use fully qualified names for event type keys, defaults to false
}

// EventBus is an implementation of the Bus interface
type EventBus struct {
	config   Config
	handlers map[string][]HandlerFunc
	mu       sync.RWMutex
	sem      chan struct{}
}

// New creates a new EventBus with the given configuration
// if no configuration is provided, defaults are used
func New(config ...Config) *EventBus {
	var cfg Config
	if len(config) > 0 {
		cfg = config[0]
	}
	cfg = makeConfig(cfg)
	return &EventBus{
		config:   cfg,
		handlers: map[string][]HandlerFunc{},
		sem:      make(chan struct{}, cfg.MaxConcurrentHandlers),
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
				b.handlers[k] = append(r[:i], r[i+1:]...)

				if len(b.handlers[k]) == 0 { // clean up empty slice
					delete(b.handlers, k)
				}
				return
			}
		}

	}
}

// Emit emits an event where handler functions are invoked sequentially in a single goroutine
// non-blocking unless the max concurrency limit is reached
// panics if no handlers are registered for the event type
func (b *EventBus) Emit(event any) {
	k := makeTypeKey(event, b.config.UseFullyQualifiedNames)

	b.mu.RLock()
	handlers := append([]HandlerFunc{}, b.handlers[k]...)
	b.mu.RUnlock()

	if len(handlers) == 0 {
		panic(fmt.Sprintf("relay: no handlers for event type '%s'", k))
	}

	b.sem <- struct{}{} // acquire, block if maxConcurrentHandlers reached
	go func() {
		defer func() { <-b.sem }()
		for _, h := range handlers {
			h(event)
		}
	}()
}

// EmitConcurrent emits an event concurrently where all handler functions are invoked in
// their own goroutine
// non-blocking unless the max concurrency limit is reached
// panics if no handlers are registered for the event type
func (b *EventBus) EmitConcurrent(event any) {
	k := makeTypeKey(event, b.config.UseFullyQualifiedNames)

	b.mu.RLock()
	handlers := append([]HandlerFunc{}, b.handlers[k]...)
	b.mu.RUnlock()

	if len(handlers) == 0 {
		panic(fmt.Sprintf("relay: no handlers for event type '%s'", k))
	}

	for _, h := range handlers {
		b.sem <- struct{}{} // acquire, block if maxConcurrentHandlers reached
		// concurrent emit
		go func(fn HandlerFunc) {
			defer func() { <-b.sem }()
			fn(event)
		}(h)
	}
}

// EmitSync emits an event synchronously where handler functions are invoked sequentially
// blocking until all handlers are done
// panics if no handlers are registered for the event type
func (b *EventBus) EmitSync(event any) {
	k := makeTypeKey(event, b.config.UseFullyQualifiedNames)

	b.mu.RLock()
	handlers := append([]HandlerFunc{}, b.handlers[k]...)
	b.mu.RUnlock()

	if len(handlers) == 0 {
		panic(fmt.Sprintf("relay: no handlers for event type '%s'", k))
	}

	for _, h := range handlers {
		h(event)
	}
}

// Handle registers a handler for the given event type
// panics if the event type is not a named struct or pointer to a named struct
// panics if the fn is nil
func (b *EventBus) Handle(event any, fn HandlerFunc) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if fn == nil {
		panic("relay: handler function must not be nil")
	}
	k := makeTypeKey(event, b.config.UseFullyQualifiedNames)
	if _, ok := b.handlers[k]; !ok {
		b.handlers[k] = []HandlerFunc{}
	}
	b.handlers[k] = append(b.handlers[k], fn)
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

// makeConfig applies defaults to the config
func makeConfig(config Config) Config {
	if config.MaxConcurrentHandlers <= 0 {
		config.MaxConcurrentHandlers = 4
	}
	return config
}

// makeTypeKey returns a string key for the type of v
// panics if v is nil
// panics if v is not a named struct or pointer to a named struct
// if full is true, uses fully qualified names (including package path)
// if full is false, uses short names (type only)
func makeTypeKey(v any, full bool) string {
	if v == nil {
		panic("relay: event must not be nil")
	}
	if !full {
		return fmt.Sprintf("%T", v)
	}
	t := reflect.TypeOf(v)
	var ptr string
	switch t.Kind() {
	case reflect.Struct:
		// ok
	case reflect.Pointer:
		if t.Elem().Kind() != reflect.Struct {
			panic(fmt.Sprintf(
				"relay: event must be a struct or pointer to struct, got pointer to '%s'",
				t.Elem().Kind(),
			))
		}
		ptr = "*"
		t = t.Elem()
	default:
		panic(fmt.Sprintf("relay: event must be a struct or pointer to struct, got '%s'", t.Kind()))
	}
	name := t.Name()
	if name == "" {
		panic(
			fmt.Sprintf(
				"relay: event must be a named struct or pointer to named struct, got '%s'", t,
			),
		)
	}
	return ptr + t.PkgPath() + "." + name
}
