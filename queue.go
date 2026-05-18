package relay

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
)

const (
	// queueDefaultBufferSize is the default buffer size for the event queue
	queueDefaultBufferSize = 128
)

var (
	// ErrInvalidSubscriber indicates that a subscriber function is invalid
	ErrInvalidSubscriber = errors.New("relay: invalid subscriber")
	// ErrNoSubscribers indicates that there are no subscribers for the event type
	ErrNoSubscribers = errors.New("relay: no subscribers")
	// ErrQueueClosed indicates that the queue is closed and cannot accept new events
	ErrQueueClosed = errors.New("relay: queue is closed")
	// ErrQueueFull indicates that the internal queue buffer is full
	ErrQueueFull = errors.New("relay: queue is full")
)

// Publisher is an interface for publishing events
type Publisher interface {
	// Publish publishes an event to the queue
	// returns ErrInvalidEvent if the event is invalid
	// returns ErrNoSubscribers if there are no subscribers for the event type
	// returns ErrQueueFull if the internal queue buffer is full
	Publish(Event) error
}

// Subscriber is an interface for subscribing to events
type Subscriber interface {
	// Subscribe subscribes a callback function to an event type
	// returns ErrInvalidEvent if the event is invalid
	Subscribe(Event, SubscriberFunc) error
}

// SubscriberFunc is a function that processes an event
// event type must be a named struct or pointer to a named struct
// a struct vs a pointer to a struct are considered different types
type SubscriberFunc = func(Event)

// NewSubscriberFunc creates a SubscriberFunc for the given function
// the function must accept a single argument of type T
// T must be a named struct or pointer to a named struct
// a struct vs a pointer to a struct are considered different types
// panics if the function is nil or does not match the expected signature
func NewSubscriberFunc[T Event](fn func(event T)) (T, SubscriberFunc) {
	if fn == nil {
		panic("relay: subscriber function must not be nil")
	}
	var t T
	return t, func(event Event) {
		e, ok := event.(T)
		if !ok {
			panic(fmt.Sprintf(
				"relay: subscriber expected event of type '%T', got '%T'", *new(T), event,
			))
		}
		fn(e)
	}
}

// Queue is an interface for an event queue
type Queue interface {
	Publisher
	Subscriber
}

// QueueOptions are the options for an event queue
type QueueOptions struct {
	BufferSize             int  // size of the internal buffer for events, defaults to 128
	UseFullyQualifiedNames bool // use fully qualified names for event type keys, defaults to false
}

// setDefaultQueueOptions sets the default values for QueueOptions
func setDefaultQueueOptions(options QueueOptions) QueueOptions {
	if options.BufferSize <= 0 {
		options.BufferSize = queueDefaultBufferSize
	}
	return options
}

// EventQueue is an implementation of the Queue interface
type EventQueue struct {
	ch      chan resolvedEvent
	closed  bool
	closeMu sync.RWMutex
	opts    QueueOptions
	mu      sync.RWMutex
	subs    map[string][]SubscriberFunc
}

// NewQueue creates a new EventQueue with the given options
// if no options are provided, defaults are used
func NewQueue(options ...QueueOptions) *EventQueue {
	var opts QueueOptions
	if len(options) > 0 {
		opts = options[0]
	}
	opts = setDefaultQueueOptions(opts)
	return &EventQueue{
		ch:   make(chan resolvedEvent, opts.BufferSize),
		opts: opts,
		subs: map[string][]SubscriberFunc{},
	}
}

// Close closes the event queue and releases any resources
// after calling Close, the queue will no longer accept new events, buffered events already
// queued may still be delivered
// returns ErrQueueClosed if the queue is already closed
func (q *EventQueue) Close() error {
	q.closeMu.Lock()
	defer q.closeMu.Unlock()

	if q.closed {
		return ErrQueueClosed
	}

	q.closed = true
	close(q.ch)
	return nil
}

// Publish publishes an event to the queue
// returns ErrInvalidEvent if the event is invalid
// returns ErrNoSubscribers if there are no subscribers for the event type
// returns ErrQueueFull if the internal queue buffer is full
// returns ErrQueueClosed if the queue is closed
func (q *EventQueue) Publish(e Event) error {
	q.closeMu.RLock()
	defer q.closeMu.RUnlock()

	if q.closed {
		return ErrQueueClosed
	}

	k, err := resolveEventKey(e, q.opts.UseFullyQualifiedNames)
	if err != nil {
		return err
	}

	q.mu.RLock()
	subs, ok := q.subs[k]
	q.mu.RUnlock()

	if !ok || len(subs) == 0 {
		return fmt.Errorf("%w: %s", ErrNoSubscribers, k)
	}

	select {
	case q.ch <- resolvedEvent{
		key:   k,
		event: e,
	}:
		return nil
	default:
		return ErrQueueFull
	}
}

// Run runs the event queue and processes events until the context is canceled
// or the queue is closed
// returns error if there are no subscribers for an event type
func (q *EventQueue) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil

		case r, ok := <-q.ch:
			if !ok {
				return nil
			}

			q.mu.RLock()
			subs, ok := q.subs[r.key]
			q.mu.RUnlock()

			if !ok || len(subs) == 0 {
				return fmt.Errorf("%w: %s", ErrNoSubscribers, r.key)
			}

			for _, sub := range subs {
				sub(r.event)
			}
		}
	}
}

// Subscribe registers a subscriber function for the given event type on the provided subscriber
// returns ErrInvalidEvent if the event is invalid
func (q *EventQueue) Subscribe(e Event, fn SubscriberFunc) error {
	k, err := resolveEventKey(e, q.opts.UseFullyQualifiedNames)
	if err != nil {
		return err
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	// copy-on-write to preserve immutable subscriber snapshots for Run
	old := q.subs[k]
	next := make([]SubscriberFunc, len(old)+1)
	copy(next, old)
	next[len(old)] = fn
	q.subs[k] = next

	return nil
}

// Unsubscribe removes a subscriber function for the given event type on the provided subscriber
// if the subscriber function is not found, it does nothing
func (q *EventQueue) Unsubscribe(e Event, fn SubscriberFunc) error {
	k, err := resolveEventKey(e, q.opts.UseFullyQualifiedNames)
	if err != nil {
		return err
	}

	fnID := reflect.ValueOf(fn).Pointer()

	q.mu.Lock()
	defer q.mu.Unlock()

	subs, ok := q.subs[k]
	if !ok {
		return nil
	}

	for i, sub := range subs {
		if reflect.ValueOf(sub).Pointer() == fnID {
			// copy-on-write to preserve immutable subscriber snapshots for Run
			next := make([]SubscriberFunc, 0, len(subs)-1)
			next = append(next, subs[:i]...)
			next = append(next, subs[i+1:]...)

			if len(next) == 0 {
				delete(q.subs, k)
			} else {
				q.subs[k] = next
			}

			return nil
		}
	}
	return nil
}

// Subscribe registers a subscriber function for the given event type on the provided subscriber
func Subscribe[T Event](s Subscriber, fn func(event T)) error {
	if s == nil {
		return fmt.Errorf("%w: subscriber must not be nil", ErrInvalidSubscriber)
	}
	e, sf := NewSubscriberFunc(fn)
	return s.Subscribe(e, sf)
}
