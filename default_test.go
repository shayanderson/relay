package relay

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestInitDefault(t *testing.T) {
	if instance == nil {
		t.Fatal("instance is nil")
	}
	if instance.get() == nil {
		t.Fatal("default bus is not set")
	}
	if Default() == nil {
		t.Fatal("Default() is nil")
	}
}

func TestSetDefault(t *testing.T) {
	b := New(Config{})
	SetDefault(b)
	if instance.get() != b {
		t.Fatal("default bus is not set correctly")
	}
	if Default() != b {
		t.Fatal("Default() is not set correctly")
	}

	defer func() {
		want := "relay: default bus is not set, use relay.SetDefault"
		if r := recover(); r != want {
			t.Fatalf("expected panic '%s', got '%v'", want, r)
		}
	}()
	instance.set(nil)
	Default()
	t.Fatal("expected panic, got none")
}

func TestDefaultEmit(t *testing.T) {
	b := New(Config{})
	SetDefault(b)
	var wg sync.WaitGroup
	var c atomic.Int32
	b.Handle(NewHandler(func(e testEvent) {
		defer wg.Done()
		c.Add(1)
	}))
	wg.Add(3)
	Emit(testEvent{})
	Emit(testEvent{})
	Emit(testEvent{})
	wg.Wait()
	if c.Load() != 3 {
		t.Fatalf("expected 3 events, got %d", c.Load())
	}
}

func TestDefaultEmitSync(t *testing.T) {
	b := New(Config{})
	SetDefault(b)
	var c atomic.Int32
	b.Handle(NewHandler(func(e testEvent) {
		c.Add(1)
	}))
	EmitSync(testEvent{})
	EmitSync(testEvent{})
	EmitSync(testEvent{})
	if c.Load() != 3 {
		t.Fatalf("expected 3 events, got %d", c.Load())
	}
}

func TestDefaultHandle(t *testing.T) {
	b := New(Config{})
	SetDefault(b)
	var n atomic.Int32
	Handle(b, func(e testEvent) {
		n.Add(1)
	})
	EmitSync(testEvent{})
	EmitSync(testEvent{})
	EmitSync(testEvent{})
	if n.Load() != 3 {
		t.Fatalf("expected 3 events, got %d", n.Load())
	}
}

func TestDefaultHandle_bus(t *testing.T) {
	b := New(Config{})
	var n atomic.Int32
	Handle(b, func(e testEvent) {
		n.Add(1)
	})
	b.EmitSync(testEvent{})
	b.EmitSync(testEvent{})
	b.EmitSync(testEvent{})
	if n.Load() != 3 {
		t.Fatalf("expected 3 events, got %d", n.Load())
	}
}

func TestDefaultHandle_nilBus(t *testing.T) {
	defer func() {
		want := "relay: bus cannot be nil"
		if r := recover(); r != want {
			t.Fatalf("expected panic '%s', got '%v'", want, r)
		}
	}()
	Handle(nil, func(e testEvent) {})
	t.Fatal("expected panic, got none")
}

func TestDefaultHandle_nilHandler(t *testing.T) {
	b := New(Config{})
	SetDefault(b)
	defer func() {
		want := "relay: handler function must not be nil"
		if r := recover(); r != want {
			t.Fatalf("expected panic '%s', got '%v'", want, r)
		}
	}()
	Handle[any](b, nil)
	t.Fatal("expected panic, got none")
}

func TestDefaultHandle_wrongEventType(t *testing.T) {
	b := New(Config{})
	SetDefault(b)
	defer func() {
		want := "relay: no handlers for event type 'int'"
		if r := recover(); r != want {
			t.Fatalf("expected panic '%s', got '%v'", want, r)
		}
	}()
	Handle(b, func(e testEvent) {})
	EmitSync(123)
	t.Fatal("expected panic, got none")
}

func TestDefaultOn(t *testing.T) {
	b := New(Config{})
	SetDefault(b)
	var n atomic.Int32
	On(func(e testEvent) {
		n.Add(1)
	})
	EmitSync(testEvent{})
	EmitSync(testEvent{})
	EmitSync(testEvent{})
	if n.Load() != 3 {
		t.Fatalf("expected 3 events, got %d", n.Load())
	}
}
