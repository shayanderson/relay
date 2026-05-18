package relay

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type busTestEvent struct {
	cancel context.CancelFunc
}

func TestNewHandlerFunc(t *testing.T) {
	t.Parallel()

	type localEvent struct{ name string }
	v, h := NewHandlerFunc(func(event localEvent) {
		if event.name != "test" {
			t.Fatalf("expected event 'test', got '%s'", event.name)
		}
	})

	if reflect.TypeOf(v).Kind() != reflect.TypeOf(localEvent{}).Kind() {
		t.Fatalf("expected type 'localEvent', got '%T'", v)
	}

	defer func() {
		want := "relay: handler expected event of type 'relay.localEvent', got 'int'"
		if r := recover(); r != want {
			t.Fatalf("expected panic '%s', got '%v'", want, r)
		}
	}()
	h(123)
	t.Fatal("expected panic, got none")
}

func TestNewBus(t *testing.T) {
	t.Parallel()

	b := NewBus()
	if b == nil {
		t.Fatal("expected bus, got nil")
	}
	if b.handlers == nil {
		t.Fatal("expected handlers map, got nil")
	}
	if len(b.handlers) != 0 {
		t.Fatalf("expected empty handlers map, got %d", len(b.handlers))
	}
}

func TestEventBusCancel(t *testing.T) {
	t.Parallel()

	b := NewBus()
	_, h1 := NewHandlerFunc(func(e busTestEvent) {})
	_, h2 := NewHandlerFunc(func(e busTestEvent) {})
	_, h3 := NewHandlerFunc(func(e busTestEvent) {})

	if err := b.Handle(busTestEvent{}, h1); err != nil {
		t.Fatalf("handle h1 failed: %v", err)
	}
	if err := b.Handle(busTestEvent{}, h2); err != nil {
		t.Fatalf("handle h2 failed: %v", err)
	}
	if err := b.Handle(busTestEvent{}, h3); err != nil {
		t.Fatalf("handle h3 failed: %v", err)
	}
	k, err := resolveEventKey(busTestEvent{}, b.opts.UseFullyQualifiedNames)
	if err != nil {
		t.Fatalf("resolve key failed: %v", err)
	}

	type busTestEvent2 struct{}
	_, h4 := NewHandlerFunc(func(e busTestEvent2) {})
	if err := b.Handle(busTestEvent2{}, h4); err != nil {
		t.Fatalf("handle h4 failed: %v", err)
	}
	k2, err := resolveEventKey(busTestEvent2{}, b.opts.UseFullyQualifiedNames)
	if err != nil {
		t.Fatalf("resolve key2 failed: %v", err)
	}

	if len(b.handlers[k]) != 3 {
		t.Fatalf("expected 3 handlers, got %d", len(b.handlers[k]))
	}
	if len(b.handlers[k2]) != 1 {
		t.Fatalf("expected 1 handler, got %d", len(b.handlers[k2]))
	}

	b.Cancel(h2)
	if len(b.handlers[k]) != 2 {
		t.Fatalf("expected 2 handlers, got %d", len(b.handlers[k]))
	}
	if reflect.ValueOf(b.handlers[k][0]).Pointer() != reflect.ValueOf(h1).Pointer() {
		t.Fatal("expected h1 as first handler")
	}
	if reflect.ValueOf(b.handlers[k][1]).Pointer() != reflect.ValueOf(h3).Pointer() {
		t.Fatal("expected h3 as second handler")
	}

	b.Cancel(h4)
	if _, ok := b.handlers[k2]; ok {
		t.Fatalf("expected no handlers for %q", k2)
	}

	b.Cancel(h1)
	b.Cancel(h3)
	if len(b.handlers) != 0 {
		t.Fatalf("expected 0 handlers, got %d", len(b.handlers))
	}
}

func TestEventBusEmit(t *testing.T) {
	t.Parallel()

	b := NewBus()
	var n atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	_, hf := NewHandlerFunc(func(e busTestEvent) {
		n.Add(1)
		if n.Load() == 3 {
			e.cancel()
		}
	})
	if err := b.Handle(busTestEvent{}, hf); err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	b.Emit(busTestEvent{cancel: cancel})
	b.Emit(busTestEvent{cancel: cancel})
	b.Emit(busTestEvent{cancel: cancel})

	<-ctx.Done()
	if n.Load() != 3 {
		t.Fatalf("expected 3 events, got %d", n.Load())
	}
}

func TestEventBusEmitMaxHandlers(t *testing.T) {
	t.Parallel()

	b := NewBus(BusOptions{MaxConcurrentHandlers: 1})
	b.sem <- struct{}{}

	var n atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	_, hf := NewHandlerFunc(func(e busTestEvent) {
		n.Add(1)
		if n.Load() == 3 {
			e.cancel()
		}
	})
	if err := b.Handle(busTestEvent{}, hf); err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	go func() {
		b.Emit(busTestEvent{cancel: cancel})
		b.Emit(busTestEvent{cancel: cancel})
		b.Emit(busTestEvent{cancel: cancel})
	}()

	time.Sleep(time.Millisecond)
	if n.Load() != 0 {
		t.Fatalf("expected 0 events, got %d", n.Load())
	}
	<-b.sem
	<-ctx.Done()
	if n.Load() != 3 {
		t.Fatalf("expected 3 events, got %d", n.Load())
	}
}

func TestEventBusEmitNoHandler(t *testing.T) {
	t.Parallel()

	b := NewBus(BusOptions{UseFullyQualifiedNames: true})
	wantErr := "relay: no handlers: github.com/shayanderson/relay.busTestEvent"
	err := b.Emit(busTestEvent{})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() != wantErr {
		t.Fatalf("expected error '%s', got '%v'", wantErr, err)
	}
}

func TestEventBusEmitInvalidEventError(t *testing.T) {
	t.Parallel()

	b := NewBus()
	wantErr := "relay: invalid event: event must not be nil"
	err := b.Emit(nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() != wantErr {
		t.Fatalf("expected error '%s', got '%v'", wantErr, err)
	}
}

func TestEventBusEmitMultipleHandlers(t *testing.T) {
	t.Parallel()

	b := NewBus()
	var n atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())

	_, h1 := NewHandlerFunc(func(e busTestEvent) {
		n.Add(1)
		if n.Load() == 9 {
			e.cancel()
		}
	})
	_, h2 := NewHandlerFunc(func(e busTestEvent) {
		n.Add(2)
		if n.Load() == 9 {
			e.cancel()
		}
	})

	_ = b.Handle(busTestEvent{}, h1)
	_ = b.Handle(busTestEvent{}, h2)

	b.Emit(busTestEvent{cancel: cancel})
	b.Emit(busTestEvent{cancel: cancel})
	b.Emit(busTestEvent{cancel: cancel})

	<-ctx.Done()
	if n.Load() != 9 {
		t.Fatalf("expected 9 events, got %d", n.Load())
	}
}

func TestEventBusEmitConcurrent(t *testing.T) {
	t.Parallel()

	b := NewBus()
	var n atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	_, hf := NewHandlerFunc(func(e busTestEvent) {
		n.Add(1)
		if n.Load() == 3 {
			e.cancel()
		}
	})
	_ = b.Handle(busTestEvent{}, hf)

	b.EmitConcurrent(busTestEvent{cancel: cancel})
	b.EmitConcurrent(busTestEvent{cancel: cancel})
	b.EmitConcurrent(busTestEvent{cancel: cancel})

	<-ctx.Done()
	if n.Load() != 3 {
		t.Fatalf("expected 3 events, got %d", n.Load())
	}
}

func TestEventBusEmitConcurrentMaxHandlers(t *testing.T) {
	t.Parallel()

	b := NewBus(BusOptions{MaxConcurrentHandlers: 1})
	b.sem <- struct{}{}

	var n atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	_, hf := NewHandlerFunc(func(e busTestEvent) {
		n.Add(1)
		if n.Load() == 3 {
			e.cancel()
		}
	})
	_ = b.Handle(busTestEvent{}, hf)

	go func() {
		b.EmitConcurrent(busTestEvent{cancel: cancel})
		b.EmitConcurrent(busTestEvent{cancel: cancel})
		b.EmitConcurrent(busTestEvent{cancel: cancel})
	}()

	time.Sleep(time.Millisecond)
	if n.Load() != 0 {
		t.Fatalf("expected 0 events, got %d", n.Load())
	}
	<-b.sem
	<-ctx.Done()
	if n.Load() != 3 {
		t.Fatalf("expected 3 events, got %d", n.Load())
	}
}

func TestEventBusEmitConcurrentNoHandler(t *testing.T) {
	t.Parallel()

	b := NewBus(BusOptions{UseFullyQualifiedNames: true})
	wantErr := "relay: no handlers: github.com/shayanderson/relay.busTestEvent"
	err := b.EmitConcurrent(busTestEvent{})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() != wantErr {
		t.Fatalf("expected error '%s', got '%v'", wantErr, err)
	}
}

func TestEventBusEmitConcurrentInvalidEventError(t *testing.T) {
	t.Parallel()

	b := NewBus()
	err := b.EmitConcurrent(nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("expected ErrInvalidEvent, got %v", err)
	}
}

func TestEventBusEmitSync(t *testing.T) {
	t.Parallel()

	b := NewBus()
	var n atomic.Int32
	_, hf := NewHandlerFunc(func(e busTestEvent) { n.Add(1) })
	_ = b.Handle(busTestEvent{}, hf)

	b.EmitSync(busTestEvent{})
	b.EmitSync(busTestEvent{})
	b.EmitSync(busTestEvent{})

	if n.Load() != 3 {
		t.Fatalf("expected 3 events, got %d", n.Load())
	}
}

func TestEventBusEmitSyncNoHandler(t *testing.T) {
	t.Parallel()

	b := NewBus(BusOptions{UseFullyQualifiedNames: true})
	wantErr := "relay: no handlers: github.com/shayanderson/relay.busTestEvent"
	err := b.EmitSync(busTestEvent{})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() != wantErr {
		t.Fatalf("expected error '%s', got '%v'", wantErr, err)
	}
}

func TestEventBusEmitSyncInvalidEventError(t *testing.T) {
	t.Parallel()

	b := NewBus()
	err := b.EmitSync(nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("expected ErrInvalidEvent, got %v", err)
	}

	b.EmitSync(nil)
}

func TestEventBusHandleNilHandler(t *testing.T) {
	t.Parallel()

	b := NewBus()
	err := b.Handle(busTestEvent{}, nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, ErrInvalidHandler) {
		t.Fatalf("expected ErrInvalidHandler, got %v", err)
	}
}

func TestEventBusHandlers(t *testing.T) {
	t.Parallel()

	b := NewBus(BusOptions{UseFullyQualifiedNames: true})
	_, h1 := NewHandlerFunc(func(e busTestEvent) {})
	_, h2 := NewHandlerFunc(func(e busTestEvent) {})
	_ = b.Handle(busTestEvent{}, h1)
	_ = b.Handle(busTestEvent{}, h2)

	type busTestEvent2 struct{}
	_, h3 := NewHandlerFunc(func(e busTestEvent2) {})
	_ = b.Handle(busTestEvent2{}, h3)

	handlers := b.Handlers()
	if len(handlers) != 2 {
		t.Fatalf("expected 2 handler keys, got %d", len(handlers))
	}

	k1 := "github.com/shayanderson/relay.busTestEvent"
	k2 := "github.com/shayanderson/relay.busTestEvent2"
	if len(handlers[k1]) != 2 {
		t.Fatalf("expected 2 handlers for %q, got %d", k1, len(handlers[k1]))
	}
	if len(handlers[k2]) != 1 {
		t.Fatalf("expected 1 handler for %q, got %d", k2, len(handlers[k2]))
	}

	handlers[k1] = nil
	if got := len(b.Handlers()[k1]); got != 2 {
		t.Fatalf("expected original handlers unchanged, got %d", got)
	}
}

func TestSetDefaultBusOptions(t *testing.T) {
	t.Parallel()

	o := setDefaultBusOptions(BusOptions{})
	if o.MaxConcurrentHandlers != busDefaultMaxConcurrentHandlers {
		t.Fatalf(
			"expected default max handlers %d, got %d",
			busDefaultMaxConcurrentHandlers,
			o.MaxConcurrentHandlers,
		)
	}

	o = setDefaultBusOptions(BusOptions{MaxConcurrentHandlers: -1})
	if o.MaxConcurrentHandlers != busDefaultMaxConcurrentHandlers {
		t.Fatalf(
			"expected default max handlers %d, got %d",
			busDefaultMaxConcurrentHandlers,
			o.MaxConcurrentHandlers,
		)
	}

	o = setDefaultBusOptions(BusOptions{MaxConcurrentHandlers: 8})
	if o.MaxConcurrentHandlers != 8 {
		t.Fatalf("expected 8, got %d", o.MaxConcurrentHandlers)
	}
}

func TestHandleHelper(t *testing.T) {
	t.Parallel()

	err := Handle(nil, func(busTestEvent) {})
	if err == nil {
		t.Fatal("expected error for nil handler")
	}
	if !errors.Is(err, ErrInvalidHandler) {
		t.Fatalf("expected ErrInvalidHandler, got %v", err)
	}
}

func TestNewHandlerFuncNilPanics(t *testing.T) {
	t.Parallel()

	defer func() {
		want := "relay: handler function must not be nil"
		if r := recover(); r != want {
			t.Fatalf("expected panic %q, got %v", want, r)
		}
	}()

	NewHandlerFunc[busTestEvent](nil)
	t.Fatal("expected panic, got none")
}

func TestEventBusHandleInvalidEvent(t *testing.T) {
	t.Parallel()

	b := NewBus()
	err := b.Handle(nil, func(Event) {})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

type handleHelperStub struct {
	called bool
	e      Event
	fn     HandlerFunc
	err    error
}

func (s *handleHelperStub) Handle(e Event, fn HandlerFunc) error {
	s.called = true
	s.e = e
	s.fn = fn
	return s.err
}

func TestHandleHelperDelegatesToHandler(t *testing.T) {
	t.Parallel()

	stub := &handleHelperStub{}
	err := Handle(stub, func(e busTestEvent) {})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if !stub.called {
		t.Fatal("expected underlying handler to be called")
	}
	if stub.fn == nil {
		t.Fatal("expected wrapped handler function to be non-nil")
	}
}

func BenchmarkEmit(b *testing.B) {
	for _, n := range []int{10, 100, 1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("handlers=%d", n), func(b *testing.B) {
			bus := NewBus()
			wg := sync.WaitGroup{}
			var c atomic.Int32
			for range n {
				_, hf := NewHandlerFunc(func(e busTestEvent) {
					defer wg.Done()
					c.Add(1)
				})
				_ = bus.Handle(busTestEvent{}, hf)
			}
			b.ResetTimer()
			for b.Loop() {
				wg.Add(n)
				if err := bus.Emit(busTestEvent{}); err != nil {
					b.Fatalf("emit failed: %v", err)
				}
			}
			b.StopTimer()
			wg.Wait()
			if got, want := c.Load(), int32(b.N*n); got != want {
				b.Fatalf("expected %d events, got %d", want, got)
			}
		})
	}
}

func BenchmarkEmitConcurrent(b *testing.B) {
	for _, n := range []int{10, 100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("handlers=%d", n), func(b *testing.B) {
			bus := NewBus()
			wg := sync.WaitGroup{}
			var c atomic.Int32
			for range n {
				_, hf := NewHandlerFunc(func(e busTestEvent) {
					defer wg.Done()
					c.Add(1)
				})
				_ = bus.Handle(busTestEvent{}, hf)
			}
			b.ResetTimer()
			for b.Loop() {
				wg.Add(n)
				err := bus.EmitConcurrent(busTestEvent{})
				if err != nil {
					b.Fatalf("emit concurrent failed: %v", err)
				}
			}
			b.StopTimer()
			wg.Wait()
			if got, want := c.Load(), int32(b.N*n); got != want {
				b.Fatalf("expected %d events, got %d", want, got)
			}
		})
	}
}

func BenchmarkEmitSync(b *testing.B) {
	for _, n := range []int{10, 100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("handlers=%d", n), func(b *testing.B) {
			bus := NewBus()
			var c atomic.Int32
			for range n {
				_, hf := NewHandlerFunc(func(e busTestEvent) { c.Add(1) })
				_ = bus.Handle(busTestEvent{}, hf)
			}
			b.ResetTimer()
			for b.Loop() {
				bus.EmitSync(busTestEvent{})
			}
			b.StopTimer()
			if got, want := c.Load(), int32(b.N*n); got != want {
				b.Fatalf("expected %d events, got %d", want, got)
			}
		})
	}
}
