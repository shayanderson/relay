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
		want := "default bus is not set, use relay.SetDefault"
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
	Handle(func(e testEvent) {
		n.Add(1)
	})
	EmitSync(testEvent{})
	EmitSync(testEvent{})
	EmitSync(testEvent{})
	if n.Load() != 3 {
		t.Fatalf("expected 3 events, got %d", n.Load())
	}
}
