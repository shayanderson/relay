package relay

import (
	"errors"
	"fmt"
	"reflect"
)

// ErrInvalidEvent indicates that an event is invalid
// such as being nil or not a named struct or pointer to a named struct
var ErrInvalidEvent = errors.New("relay: invalid event")

// Event represents an event
type Event any

// resolvedEvent represents an event with its resolved type key
type resolvedEvent struct {
	key   string
	event Event
}

// resolveEventKey returns a string key for the type of v
// returns error if e is nil
// returns error if e is not a named struct or pointer to a named struct
// if full is true, uses fully qualified names (including package path)
// if full is false, uses short names (type only)
func resolveEventKey(e Event, full bool) (string, error) {
	if e == nil {
		return "", fmt.Errorf("%w: event must not be nil", ErrInvalidEvent)
	}
	if !full {
		return fmt.Sprintf("%T", e), nil
	}
	t := reflect.TypeOf(e)
	var ptr string
	switch t.Kind() {
	case reflect.Struct:
		// ok
	case reflect.Pointer:
		if t.Elem().Kind() != reflect.Struct {
			return "", fmt.Errorf(
				"%w: event must be a struct or pointer to struct, got pointer to '%s'",
				ErrInvalidEvent, t.Elem().Kind(),
			)
		}
		ptr = "*"
		t = t.Elem()
	default:
		return "", fmt.Errorf(
			"%w: event must be a struct or pointer to struct, got '%s'", ErrInvalidEvent, t.Kind(),
		)
	}
	name := t.Name()
	if name == "" {
		return "", fmt.Errorf(
			"%w: event must be a named struct or pointer to named struct, got '%s'",
			ErrInvalidEvent, t,
		)
	}
	return ptr + t.PkgPath() + "." + name, nil
}
