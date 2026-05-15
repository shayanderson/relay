package relay

import (
	"errors"
	"testing"
)

type eventTestNamed struct{}

func TestResolveEventKey_NotFullUsesTypeString(t *testing.T) {
	t.Parallel()

	k, err := resolveEventKey(eventTestNamed{}, false)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if k != "relay.eventTestNamed" {
		t.Fatalf("expected key %q, got %q", "relay.eventTestNamed", k)
	}

	k, err = resolveEventKey(&eventTestNamed{}, false)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if k != "*relay.eventTestNamed" {
		t.Fatalf("expected key %q, got %q", "*relay.eventTestNamed", k)
	}
}

func TestResolveEventKey_FullNamedStruct(t *testing.T) {
	t.Parallel()

	k, err := resolveEventKey(eventTestNamed{}, true)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if k != "github.com/shayanderson/relay.eventTestNamed" {
		t.Fatalf("expected full key for named struct, got %q", k)
	}
}

func TestResolveEventKey_FullNamedPointerStruct(t *testing.T) {
	t.Parallel()

	k, err := resolveEventKey(&eventTestNamed{}, true)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if k != "*github.com/shayanderson/relay.eventTestNamed" {
		t.Fatalf("expected full key for named pointer struct, got %q", k)
	}
}

func TestResolveEventKey_NilEvent(t *testing.T) {
	t.Parallel()

	_, err := resolveEventKey(nil, true)
	if err == nil {
		t.Fatal("expected error for nil event")
	}
	if !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("expected ErrInvalidEvent, got %v", err)
	}
	if err.Error() != "relay: invalid event: event must not be nil" {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestResolveEventKey_NonStructEvent(t *testing.T) {
	t.Parallel()

	_, err := resolveEventKey(123, true)
	if err == nil {
		t.Fatal("expected error for non-struct event")
	}
	if !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("expected ErrInvalidEvent, got %v", err)
	}
	if err.Error() != "relay: invalid event: event must be a struct or pointer to struct, got 'int'" {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestResolveEventKey_PointerToNonStructEvent(t *testing.T) {
	t.Parallel()

	v := new(int)
	_, err := resolveEventKey(v, true)
	if err == nil {
		t.Fatal("expected error for pointer to non-struct event")
	}
	if !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("expected ErrInvalidEvent, got %v", err)
	}
	if err.Error() != "relay: invalid event: event must be a struct or pointer to struct, got pointer to 'int'" {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestResolveEventKey_NonNamedStructEvent(t *testing.T) {
	t.Parallel()

	_, err := resolveEventKey(struct{}{}, true)
	if err == nil {
		t.Fatal("expected error for non-named struct event")
	}
	if !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("expected ErrInvalidEvent, got %v", err)
	}
	if err.Error() != "relay: invalid event: event must be a named struct or pointer to named struct, got 'struct {}'" {
		t.Fatalf("unexpected error message: %v", err)
	}
}
