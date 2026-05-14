package relay

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type testEvent struct {
	cancel context.CancelFunc
}

func TestNewHandlerFunc(t *testing.T) {
	type testEvent struct{ name string }
	v, h := NewHandlerFunc(func(event testEvent) {
		if event.name != "test" {
			t.Fatalf("expected event 'test', got '%s'", event.name)
		}
	})
	if reflect.TypeOf(v).Kind() != reflect.TypeOf(testEvent{}).Kind() {
		t.Fatalf("expected type 'testEvent', got '%T'", v)
	}

	defer func() {
		want := "relay: handler expected event of type 'relay.testEvent', got 'int'"
		if r := recover(); r != want {
			t.Fatalf("expected panic '%s', got '%v'", want, r)
		}
	}()
	h(123)
	t.Fatal("expected panic, got none")
}

func TestNew(t *testing.T) {
	b := New()
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

func TestBus_Cancel(t *testing.T) {
	b := New()
	_, h1 := NewHandlerFunc(func(e testEvent) {})
	_, h2 := NewHandlerFunc(func(e testEvent) {})
	_, h3 := NewHandlerFunc(func(e testEvent) {})
	b.Handle(testEvent{}, h1)
	b.Handle(testEvent{}, h2)
	b.Handle(testEvent{}, h3)
	k := makeTypeKey(testEvent{}, b.config.UseFullyQualifiedNames)

	type testEvent2 struct{}
	_, h4 := NewHandlerFunc(func(e testEvent2) {})
	b.Handle(testEvent2{}, h4)
	k2 := makeTypeKey(testEvent2{}, b.config.UseFullyQualifiedNames)

	if len(b.handlers) != 2 {
		t.Fatalf("expected 2 handlers, got %d", len(b.handlers))
	}
	if len(b.handlers[k]) != 3 {
		t.Fatalf("expected 3 handlers, got %d", len(b.handlers[k]))
	}
	if b.handlers[k][0] == nil || b.handlers[k][1] == nil || b.handlers[k][2] == nil {
		t.Fatal("expected non-nil handlers")
	}
	if reflect.ValueOf(b.handlers[k][0]).Pointer() != reflect.ValueOf(h1).Pointer() {
		t.Fatal("expected h1 as first handler")
	}
	if reflect.ValueOf(b.handlers[k][1]).Pointer() != reflect.ValueOf(h2).Pointer() {
		t.Fatal("expected h2 as second handler")
	}
	if reflect.ValueOf(b.handlers[k][2]).Pointer() != reflect.ValueOf(h3).Pointer() {
		t.Fatal("expected h3 as third handler")
	}

	if len(b.handlers[k2]) != 1 {
		t.Fatalf("expected 1 handler, got %d", len(b.handlers[k2]))
	}
	if b.handlers[k2][0] == nil {
		t.Fatal("expected non-nil handler")
	}
	if reflect.ValueOf(b.handlers[k2][0]).Pointer() != reflect.ValueOf(h4).Pointer() {
		t.Fatal("expected h4 as handler")
	}

	b.Cancel(h2)
	if len(b.handlers) != 2 {
		t.Fatalf("expected 2 handlers, got %d", len(b.handlers))
	}
	if len(b.handlers[k]) != 2 {
		t.Fatalf("expected 2 handlers, got %d", len(b.handlers[k]))
	}
	if b.handlers[k][0] == nil || b.handlers[k][1] == nil {
		t.Fatal("expected non-nil handlers")
	}
	if reflect.ValueOf(b.handlers[k][0]).Pointer() != reflect.ValueOf(h1).Pointer() {
		t.Fatal("expected h1 as first handler")
	}
	if reflect.ValueOf(b.handlers[k][1]).Pointer() != reflect.ValueOf(h3).Pointer() {
		t.Fatal("expected h3 as second handler")
	}
	if len(b.handlers[k2]) != 1 {
		t.Fatalf("expected 1 handler, got %d", len(b.handlers[k2]))
	}
	if b.handlers[k2][0] == nil {
		t.Fatal("expected non-nil handler")
	}
	if reflect.ValueOf(b.handlers[k2][0]).Pointer() != reflect.ValueOf(h4).Pointer() {
		t.Fatal("expected h4 as handler")
	}

	b.Cancel(h4)
	if len(b.handlers) != 1 {
		t.Fatalf("expected 1 handler, got %d", len(b.handlers))
	}
	if len(b.handlers[k]) != 2 {
		t.Fatalf("expected 2 handlers, got %d", len(b.handlers[k]))
	}
	if b.handlers[k][0] == nil || b.handlers[k][1] == nil {
		t.Fatal("expected non-nil handlers")
	}
	if reflect.ValueOf(b.handlers[k][0]).Pointer() != reflect.ValueOf(h1).Pointer() {
		t.Fatal("expected h1 as first handler")
	}
	if reflect.ValueOf(b.handlers[k][1]).Pointer() != reflect.ValueOf(h3).Pointer() {
		t.Fatal("expected h3 as second handler")
	}
	if _, ok := b.handlers[k2]; ok {
		t.Fatalf("expected no handlers, got %d", len(b.handlers[k2]))
	}

	b.Cancel(h1)
	b.Cancel(h3)
	if len(b.handlers) != 0 {
		t.Fatalf("expected 0 handlers, got %d", len(b.handlers))
	}
}

func TestBus_Emit(t *testing.T) {
	b := New()
	var n atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	h := func(e testEvent) {
		n.Add(1)
		if n.Load() == 3 {
			e.cancel()
		}
	}
	b.Handle(NewHandlerFunc(h))
	b.Emit(testEvent{cancel: cancel})
	b.Emit(testEvent{cancel: cancel})
	b.Emit(testEvent{cancel: cancel})
	<-ctx.Done()
	if n.Load() != 3 {
		t.Fatalf("expected 3 events, got %d", n.Load())
	}
}

func TestBus_Emit_maxHandlers(t *testing.T) {
	b := New(Config{MaxConcurrentHandlers: 1})
	b.sem <- struct{}{} // acquire
	var n atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	h := func(e testEvent) {
		n.Add(1)
		if n.Load() == 3 {
			e.cancel()
		}
	}
	b.Handle(NewHandlerFunc(h))
	go func() {
		b.Emit(testEvent{cancel: cancel})
		b.Emit(testEvent{cancel: cancel})
		b.Emit(testEvent{cancel: cancel})
	}()
	time.Sleep(time.Millisecond)
	if n.Load() != 0 {
		t.Fatalf("expected 0 events, got %d", n.Load())
	}
	<-b.sem // release
	<-ctx.Done()
	if n.Load() != 3 {
		t.Fatalf("expected 3 events, got %d", n.Load())
	}
}

func TestBus_Emit_noHandler(t *testing.T) {
	b := New(Config{UseFullyQualifiedNames: true})
	defer func() {
		want := "relay: no handlers for event type 'github.com/shayanderson/relay.testEvent'"
		if r := recover(); r != want {
			t.Fatalf("expected panic '%s', got '%v'", want, r)
		}
	}()
	b.Emit(testEvent{})
	t.Fatal("expected panic, got none")
}

func TestBus_Emit_multipleHandlers(t *testing.T) {
	b := New()
	var n atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	h1 := func(e testEvent) {
		n.Add(1)
		if n.Load() == 9 {
			e.cancel()
		}
	}
	h2 := func(e testEvent) {
		n.Add(2)
		if n.Load() == 9 {
			e.cancel()
		}
	}
	b.Handle(NewHandlerFunc(h1))
	b.Handle(NewHandlerFunc(h2))
	b.Emit(testEvent{cancel: cancel})
	b.Emit(testEvent{cancel: cancel})
	b.Emit(testEvent{cancel: cancel})
	<-ctx.Done()
	if n.Load() != 9 {
		t.Fatalf("expected 9 events, got %d", n.Load())
	}
}

func TestBus_EmitConcurrent(t *testing.T) {
	b := New()
	var n atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	h := func(e testEvent) {
		n.Add(1)
		if n.Load() == 3 {
			e.cancel()
		}
	}
	b.Handle(NewHandlerFunc(h))
	b.EmitConcurrent(testEvent{cancel: cancel})
	b.EmitConcurrent(testEvent{cancel: cancel})
	b.EmitConcurrent(testEvent{cancel: cancel})
	<-ctx.Done()
	if n.Load() != 3 {
		t.Fatalf("expected 3 events, got %d", n.Load())
	}
}

func TestBus_EmitConcurrent_maxHandlers(t *testing.T) {
	b := New(Config{MaxConcurrentHandlers: 1})
	b.sem <- struct{}{} // acquire
	var n atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	h := func(e testEvent) {
		n.Add(1)
		if n.Load() == 3 {
			e.cancel()
		}
	}
	b.Handle(NewHandlerFunc(h))
	go func() {
		b.EmitConcurrent(testEvent{cancel: cancel})
		b.EmitConcurrent(testEvent{cancel: cancel})
		b.EmitConcurrent(testEvent{cancel: cancel})
	}()
	time.Sleep(time.Millisecond)
	if n.Load() != 0 {
		t.Fatalf("expected 0 events, got %d", n.Load())
	}
	<-b.sem // release
	<-ctx.Done()
	if n.Load() != 3 {
		t.Fatalf("expected 3 events, got %d", n.Load())
	}
}

func TestBus_EmitConcurrent_noHandler(t *testing.T) {
	b := New(Config{UseFullyQualifiedNames: true})
	defer func() {
		want := "relay: no handlers for event type 'github.com/shayanderson/relay.testEvent'"
		if r := recover(); r != want {
			t.Fatalf("expected panic '%s', got '%v'", want, r)
		}
	}()
	b.EmitConcurrent(testEvent{})
	t.Fatal("expected panic, got none")
}

func TestBus_EmitConcurrent_multipleHandlers(t *testing.T) {
	b := New()
	var n atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	h1 := func(e testEvent) {
		n.Add(1)
		if n.Load() == 9 {
			e.cancel()
		}
	}
	h2 := func(e testEvent) {
		n.Add(2)
		if n.Load() == 9 {
			e.cancel()
		}
	}
	b.Handle(NewHandlerFunc(h1))
	b.Handle(NewHandlerFunc(h2))
	b.EmitConcurrent(testEvent{cancel: cancel})
	b.EmitConcurrent(testEvent{cancel: cancel})
	b.EmitConcurrent(testEvent{cancel: cancel})
	<-ctx.Done()
	if n.Load() != 9 {
		t.Fatalf("expected 9 events, got %d", n.Load())
	}
}

func TestBus_EmitSync(t *testing.T) {
	b := New()
	var n atomic.Int32
	h := func(e testEvent) {
		n.Add(1)
	}
	b.Handle(NewHandlerFunc(h))
	b.EmitSync(testEvent{})
	b.EmitSync(testEvent{})
	b.EmitSync(testEvent{})
	if n.Load() != 3 {
		t.Fatalf("expected 3 events, got %d", n.Load())
	}
}

func TestBus_EmitSync_noHandler(t *testing.T) {
	b := New(Config{UseFullyQualifiedNames: true})
	defer func() {
		want := "relay: no handlers for event type 'github.com/shayanderson/relay.testEvent'"
		if r := recover(); r != want {
			t.Fatalf("expected panic '%s', got '%v'", want, r)
		}
	}()
	b.EmitSync(testEvent{})
	t.Fatal("expected panic, got none")
}

func TestBus_EmitSync_multipleHandlers(t *testing.T) {
	b := New()
	var n atomic.Int32
	h1 := func(e testEvent) {
		n.Add(1)
	}
	h2 := func(e testEvent) {
		n.Add(2)
	}
	b.Handle(NewHandlerFunc(h1))
	b.Handle(NewHandlerFunc(h2))
	b.EmitSync(testEvent{})
	b.EmitSync(testEvent{})
	b.EmitSync(testEvent{})
	if n.Load() != 9 {
		t.Fatalf("expected 9 events, got %d", n.Load())
	}
}

func TestBus_Handle_nilHandler(t *testing.T) {
	b := New()
	defer func() {
		want := "relay: handler function must not be nil"
		if r := recover(); r != want {
			t.Fatalf("expected panic '%s', got '%v'", want, r)
		}
	}()
	b.Handle(testEvent{}, nil)
	t.Fatal("expected panic, got none")
}

func TestBus_Handlers(t *testing.T) {
	b := New(Config{UseFullyQualifiedNames: true})
	h1 := func(e testEvent) {}
	h2 := func(e testEvent) {}
	b.Handle(NewHandlerFunc(h1))
	b.Handle(NewHandlerFunc(h2))
	type testEvent2 struct{}
	h3 := func(e testEvent2) {}
	b.Handle(NewHandlerFunc(h3))
	handlers := b.Handlers()
	if len(handlers) != 2 {
		t.Fatalf("expected 2 handlers, got %d", len(handlers))
	}
	t1 := "github.com/shayanderson/relay.testEvent"
	t2 := "github.com/shayanderson/relay.testEvent2"
	h1s, ok := handlers[t1]
	if !ok {
		t.Fatalf("expected handlers for key '%s', got none", t1)
	}
	if len(h1s) != 2 {
		t.Fatalf("expected 2 handlers for key '%s', got %d", t1, len(h1s))
	}
	h2s, ok := handlers[t2]
	if !ok {
		t.Fatalf("expected handlers for key '%s', got none", t2)
	}
	if len(h2s) != 1 {
		t.Fatalf("expected 1 handler for key '%s', got %d", t2, len(h2s))
	}
}

func TestMakeConfig(t *testing.T) {
	c := makeConfig(Config{})
	if c.MaxConcurrentHandlers != 4 {
		t.Fatalf("expected MaxConcurrentHandlers to be 4, got %d", c.MaxConcurrentHandlers)
	}
	if c.UseFullyQualifiedNames {
		t.Fatal("expected UseFullyQualifiedNames to be false, got true")
	}

	c = makeConfig(Config{MaxConcurrentHandlers: 0})
	if c.MaxConcurrentHandlers != 4 {
		t.Fatalf("expected MaxConcurrentHandlers to be 4, got %d", c.MaxConcurrentHandlers)
	}

	c = makeConfig(Config{MaxConcurrentHandlers: -1})
	if c.MaxConcurrentHandlers != 4 {
		t.Fatalf("expected MaxConcurrentHandlers to be 4, got %d", c.MaxConcurrentHandlers)
	}

	c = makeConfig(Config{MaxConcurrentHandlers: 8})
	if c.MaxConcurrentHandlers != 8 {
		t.Fatalf("expected MaxConcurrentHandlers to be 8, got %d", c.MaxConcurrentHandlers)
	}

	c = makeConfig(Config{UseFullyQualifiedNames: true})
	if !c.UseFullyQualifiedNames {
		t.Fatal("expected UseFullyQualifiedNames to be true, got false")
	}
}

func TestMakeTypeKey(t *testing.T) {
	k := makeTypeKey(testEvent{}, true)
	want := "github.com/shayanderson/relay.testEvent"
	if k != want {
		t.Fatalf("expected type key '%s', got '%s'", want, k)
	}

	k = makeTypeKey(&testEvent{}, true)
	want = "*github.com/shayanderson/relay.testEvent"
	if k != want {
		t.Fatalf("expected type key '%s', got '%s'", want, k)
	}

	k = makeTypeKey(testEvent{}, false)
	want = "relay.testEvent"
	if k != want {
		t.Fatalf("expected type key '%s', got '%s'", want, k)
	}

	k = makeTypeKey(&testEvent{}, false)
	want = "*relay.testEvent"
	if k != want {
		t.Fatalf("expected type key '%s', got '%s'", want, k)
	}
}

func TestMakeTypeKey_nil(t *testing.T) {
	defer func() {
		want := "relay: event must not be nil"
		if r := recover(); r != want {
			t.Fatalf("expected panic '%s', got '%v'", want, r)
		}
	}()
	makeTypeKey(nil, true)
	t.Fatal("expected panic, got none")
}

func TestMakeTypeKey_nonStruct(t *testing.T) {
	defer func() {
		want := "relay: event must be a struct or pointer to struct, got 'int'"
		if r := recover(); r != want {
			t.Fatalf("expected panic '%s', got '%v'", want, r)
		}
	}()
	makeTypeKey(123, true)
	t.Fatal("expected panic, got none")
}

func TestMakeTypeKey_nonPointerStruct(t *testing.T) {
	defer func() {
		want := "relay: event must be a struct or pointer to struct, got pointer to 'int'"
		if r := recover(); r != want {
			t.Fatalf("expected panic '%s', got '%v'", want, r)
		}
	}()
	makeTypeKey(new(int), true)
	t.Fatal("expected panic, got none")
}

func TestMakeTypeKey_nonNamedStruct(t *testing.T) {
	defer func() {
		want := "relay: event must be a named struct or pointer to named struct, got 'struct {}'"
		if r := recover(); r != want {
			t.Fatalf("expected panic '%s', got '%v'", want, r)
		}
	}()
	makeTypeKey(struct{}{}, true)
	t.Fatal("expected panic, got none")
}

func BenchmarkEmit(b *testing.B) {
	for _, n := range []int{1_000, 10_000, 100_000, 1_000_000} {
		b.Run(fmt.Sprintf("handlers=%d", n), func(b *testing.B) {
			bus := New()
			wg := sync.WaitGroup{}
			var c atomic.Int32
			for range n {
				bus.Handle(NewHandlerFunc(func(e testEvent) {
					defer wg.Done()
					c.Add(1)
				}))
			}
			b.ResetTimer()
			for b.Loop() {
				wg.Add(n)
				bus.Emit(testEvent{})
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
	for _, n := range []int{1_000, 10_000, 100_000, 1_000_000} {
		b.Run(fmt.Sprintf("handlers=%d", n), func(b *testing.B) {
			bus := New()
			wg := sync.WaitGroup{}
			var c atomic.Int32
			for range n {
				bus.Handle(NewHandlerFunc(func(e testEvent) {
					defer wg.Done()
					c.Add(1)
				}))
			}
			b.ResetTimer()
			for b.Loop() {
				wg.Add(n)
				bus.EmitConcurrent(testEvent{})
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
	for _, n := range []int{1_000, 10_000, 100_000, 1_000_000} {
		b.Run(fmt.Sprintf("handlers=%d", n), func(b *testing.B) {
			bus := New()
			var c atomic.Int32
			for range n {
				bus.Handle(NewHandlerFunc(func(e testEvent) {
					c.Add(1)
				}))
			}
			b.ResetTimer()
			for b.Loop() {
				bus.EmitSync(testEvent{})
			}
			b.StopTimer()
			if got, want := c.Load(), int32(b.N*n); got != want {
				b.Fatalf("expected %d events, got %d", want, got)
			}
		})
	}
}
