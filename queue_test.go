package relay

import (
	"context"
	"errors"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type queueTestEvent struct {
	ID int
}

type queueSubscribeStub struct {
	called bool
	e      Event
	fn     SubscriberFunc
	err    error
}

func (s *queueSubscribeStub) Subscribe(e Event, fn SubscriberFunc) error {
	s.called = true
	s.e = e
	s.fn = fn
	return s.err
}

func TestNewQueue_DefaultBufferSizeApplied(t *testing.T) {
	t.Parallel()

	q := NewQueue(QueueOptions{BufferSize: 0})
	if cap(q.ch) != queueDefaultBufferSize {
		t.Fatalf("expected default buffer size %d, got %d", queueDefaultBufferSize, cap(q.ch))
	}
}

func TestNewQueue_PreservesProvidedBufferSize(t *testing.T) {
	t.Parallel()

	q := NewQueue(QueueOptions{BufferSize: 7})
	if cap(q.ch) != 7 {
		t.Fatalf("expected buffer size 7, got %d", cap(q.ch))
	}
}

func TestPublish_InvalidEvent(t *testing.T) {
	t.Parallel()

	q := NewQueue()
	err := q.Publish(nil)
	if err == nil {
		t.Fatal("expected error publishing nil event")
	}
	if !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("expected ErrInvalidEvent, got %v", err)
	}
}

func TestPublish_NoSubscribers(t *testing.T) {
	t.Parallel()

	q := NewQueue()
	err := q.Publish(queueTestEvent{ID: 1})
	if err == nil {
		t.Fatal("expected error when publishing with no subscribers")
	}
	if !errors.Is(err, ErrNoSubscribers) {
		t.Fatalf("expected ErrNoSubscribers, got %v", err)
	}
}

func TestPublish_QueueFull(t *testing.T) {
	t.Parallel()

	q := NewQueue(QueueOptions{BufferSize: 1})
	fn := func(Event) {}
	if err := q.Subscribe(queueTestEvent{}, fn); err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}

	if err := q.Publish(queueTestEvent{ID: 1}); err != nil {
		t.Fatalf("first publish failed: %v", err)
	}
	if err := q.Publish(queueTestEvent{ID: 2}); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("expected ErrQueueFull, got %v", err)
	}
}

func TestRun_DeliversEventToSubscribers(t *testing.T) {
	t.Parallel()

	q := NewQueue()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	received := make(chan queueTestEvent, 1)
	if err := q.Subscribe(queueTestEvent{}, func(e Event) {
		received <- e.(queueTestEvent)
	}); err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}

	runErr := make(chan error, 1)
	go func() {
		runErr <- q.Run(ctx)
	}()

	if err := q.Publish(queueTestEvent{ID: 42}); err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	select {
	case got := <-received:
		if got.ID != 42 {
			t.Fatalf("expected event ID 42, got %d", got.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for subscriber callback")
	}

	cancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("run returned unexpected error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for queue to stop")
	}
}

func TestRun_NoSubscribersForQueuedEventReturnsError(t *testing.T) {
	t.Parallel()

	q := NewQueue()
	k, err := resolveEventKey(queueTestEvent{}, false)
	if err != nil {
		t.Fatalf("resolve event key failed: %v", err)
	}
	q.ch <- resolvedEvent{key: k, event: queueTestEvent{ID: 7}}

	err = q.Run(t.Context())
	if err == nil {
		t.Fatal("expected run to return error with no subscribers")
	}
	if !errors.Is(err, ErrNoSubscribers) {
		t.Fatalf("expected ErrNoSubscribers, got %v", err)
	}
}

func TestRun_ReturnsNilWhenQueueClosed(t *testing.T) {
	t.Parallel()

	q := NewQueue()
	if err := q.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	if err := q.Run(t.Context()); err != nil {
		t.Fatalf("expected nil error when running closed queue, got %v", err)
	}
}

func TestClose_Idempotent(t *testing.T) {
	t.Parallel()

	q := NewQueue()
	if err := q.Close(); err != nil {
		t.Fatalf("expected no error on first close, got %v", err)
	}
	if err := q.Close(); err == nil || !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("expected ErrQueueClosed on second close, got %v", err)
	}
}

func TestSubscribe_InvalidEvent(t *testing.T) {
	t.Parallel()

	q := NewQueue()
	err := q.Subscribe(nil, func(Event) {})
	if err == nil {
		t.Fatal("expected subscribe error for nil event")
	}
	if !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("expected ErrInvalidEvent, got %v", err)
	}
}

func TestNewSubscriberFunc_NilPanics(t *testing.T) {
	t.Parallel()

	defer func() {
		want := "relay: subscriber function must not be nil"
		if r := recover(); r != want {
			t.Fatalf("expected panic %q, got %v", want, r)
		}
	}()

	NewSubscriberFunc[queueTestEvent](nil)
	t.Fatal("expected panic, got none")
}

func TestNewSubscriberFunc_TypeMismatchPanics(t *testing.T) {
	t.Parallel()

	_, sf := NewSubscriberFunc(func(e queueTestEvent) {})

	defer func() {
		want := "relay: subscriber expected event of type 'relay.queueTestEvent', got 'int'"
		if r := recover(); r != want {
			t.Fatalf("expected panic %q, got %v", want, r)
		}
	}()

	sf(123)
	t.Fatal("expected panic, got none")
}

func TestNewSubscriberFunc_Success(t *testing.T) {
	t.Parallel()

	got := make(chan int, 1)
	e, sf := NewSubscriberFunc(func(event queueTestEvent) {
		got <- event.ID
	})

	if e.ID != 0 {
		t.Fatalf("expected zero-value event, got %+v", e)
	}

	sf(queueTestEvent{ID: 77})
	select {
	case id := <-got:
		if id != 77 {
			t.Fatalf("expected ID 77, got %d", id)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for subscriber callback")
	}
}

func TestSubscribeHelper_NilSubscriber(t *testing.T) {
	t.Parallel()

	err := Subscribe[queueTestEvent](nil, func(Event) {})
	if err == nil {
		t.Fatal("expected error for nil subscriber")
	}
	if !errors.Is(err, ErrInvalidSubscriber) {
		t.Fatalf("expected ErrInvalidSubscriber, got %v", err)
	}
}

func TestSubscribeHelper_DelegatesToSubscriber(t *testing.T) {
	t.Parallel()

	stub := &queueSubscribeStub{}
	called := false
	err := Subscribe[queueTestEvent](stub, func(event Event) { called = true })
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if !stub.called {
		t.Fatal("expected subscriber Subscribe to be called")
	}
	if stub.e != nil {
		t.Fatalf("expected helper to pass zero Event (nil), got %T", stub.e)
	}
	if stub.fn == nil {
		t.Fatal("expected wrapped subscriber function to be non-nil")
	}

	stub.fn(queueTestEvent{ID: 5})
	if !called {
		t.Fatal("expected wrapped subscriber function to invoke original callback")
	}
}

func TestSubscribe_NilFunctionNotValidated(t *testing.T) {
	t.Parallel()

	q := NewQueue(QueueOptions{BufferSize: 1})
	if err := q.Subscribe(queueTestEvent{}, nil); err != nil {
		t.Fatalf("expected nil error subscribing nil function, got %v", err)
	}

	if err := q.Publish(queueTestEvent{ID: 1}); err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic when running nil subscriber callback")
		}
	}()

	_ = q.Run(t.Context())
}

func TestUnsubscribe_InvalidEvent(t *testing.T) {
	t.Parallel()

	q := NewQueue()
	err := q.Unsubscribe(nil, func(Event) {})
	if err == nil {
		t.Fatal("expected unsubscribe error for nil event")
	}
	if !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("expected ErrInvalidEvent, got %v", err)
	}
}

func TestUnsubscribe_NoOpWhenKeyOrFuncMissing(t *testing.T) {
	t.Parallel()

	q := NewQueue()
	fnA := func(Event) {}
	fnB := func(Event) {}

	if err := q.Unsubscribe(queueTestEvent{}, fnA); err != nil {
		t.Fatalf("unsubscribe on missing key failed: %v", err)
	}
	if err := q.Subscribe(queueTestEvent{}, fnA); err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}
	if err := q.Unsubscribe(queueTestEvent{}, fnB); err != nil {
		t.Fatalf("unsubscribe with missing callback failed: %v", err)
	}

	called := make(chan struct{}, 1)
	if err := q.Subscribe(queueTestEvent{}, func(Event) {
		called <- struct{}{}
	}); err != nil {
		t.Fatalf("second subscribe failed: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	runErr := make(chan error, 1)
	go func() {
		runErr <- q.Run(ctx)
	}()

	if err := q.Publish(queueTestEvent{ID: 1}); err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for subscriber callback")
	}

	cancel()
	<-runErr
}

func TestUnsubscribe_RemovesOnlyTargetSubscriber(t *testing.T) {
	t.Parallel()

	q := NewQueue()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var mu sync.Mutex
	calledA := 0
	calledB := 0

	fnA := func(Event) {
		mu.Lock()
		calledA++
		mu.Unlock()
	}
	fnB := func(Event) {
		mu.Lock()
		calledB++
		mu.Unlock()
	}

	if err := q.Subscribe(queueTestEvent{}, fnA); err != nil {
		t.Fatalf("subscribe A failed: %v", err)
	}
	if err := q.Subscribe(queueTestEvent{}, fnB); err != nil {
		t.Fatalf("subscribe B failed: %v", err)
	}
	if err := q.Unsubscribe(queueTestEvent{}, fnA); err != nil {
		t.Fatalf("unsubscribe A failed: %v", err)
	}

	runErr := make(chan error, 1)
	go func() {
		runErr <- q.Run(ctx)
	}()

	if err := q.Publish(queueTestEvent{ID: 99}); err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)
	cancel()
	<-runErr

	mu.Lock()
	defer mu.Unlock()
	if calledA != 0 {
		t.Fatalf("expected unsubscribed callback A to not be called, got %d", calledA)
	}
	if calledB != 1 {
		t.Fatalf("expected callback B to be called once, got %d", calledB)
	}
}

func TestUnsubscribe_DeletesKeyWhenLastSubscriberRemoved(t *testing.T) {
	t.Parallel()

	q := NewQueue()
	fn := func(Event) {}

	if err := q.Subscribe(queueTestEvent{}, fn); err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}

	k, err := resolveEventKey(queueTestEvent{}, false)
	if err != nil {
		t.Fatalf("resolve event key failed: %v", err)
	}
	if _, ok := q.subs[k]; !ok {
		t.Fatalf("expected key %q to exist after subscribe", k)
	}

	if err := q.Unsubscribe(queueTestEvent{}, fn); err != nil {
		t.Fatalf("unsubscribe failed: %v", err)
	}

	if _, ok := q.subs[k]; ok {
		t.Fatalf("expected key %q to be deleted after removing last subscriber", k)
	}
}

func TestPublish_Concurrent(t *testing.T) {
	t.Parallel()

	const publishers = 24
	const perPublisher = 50
	total := publishers * perPublisher

	q := NewQueue(QueueOptions{BufferSize: total})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	delivered := make(chan struct{}, total)
	if err := q.Subscribe(queueTestEvent{}, func(Event) {
		delivered <- struct{}{}
	}); err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}

	runErr := make(chan error, 1)
	go func() {
		runErr <- q.Run(ctx)
	}()

	pubErr := make(chan error, total)
	var wg sync.WaitGroup
	wg.Add(publishers)
	for i := 0; i < publishers; i++ {
		go func(base int) {
			defer wg.Done()
			for j := 0; j < perPublisher; j++ {
				if err := q.Publish(queueTestEvent{ID: base + j}); err != nil {
					pubErr <- err
					return
				}
			}
		}(i * perPublisher)
	}
	wg.Wait()
	close(pubErr)

	for err := range pubErr {
		if err != nil {
			t.Fatalf("concurrent publish failed: %v", err)
		}
	}

	for i := 0; i < total; i++ {
		select {
		case <-delivered:
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for event delivery at %d/%d", i, total)
		}
	}

	cancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("run returned unexpected error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for queue run to stop")
	}
}

func TestSubscribeUnsubscribePublish_Concurrent(t *testing.T) {
	t.Parallel()

	q := NewQueue(QueueOptions{BufferSize: 4096})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	runErr := make(chan error, 1)
	go func() {
		runErr <- q.Run(ctx)
	}()

	const workers = 8
	const loops = 150
	var received atomic.Int64

	var wg sync.WaitGroup
	wg.Add(workers)

	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < loops; j++ {
				fn := func(Event) {
					received.Add(1)
				}

				if err := q.Subscribe(queueTestEvent{}, fn); err != nil {
					t.Errorf("subscribe failed: %v", err)
					return
				}

				if err := q.Publish(queueTestEvent{ID: j}); err != nil &&
					!errors.Is(err, ErrNoSubscribers) &&
					!errors.Is(err, ErrQueueFull) {
					t.Errorf("publish failed: %v", err)
					return
				}

				if err := q.Unsubscribe(queueTestEvent{}, fn); err != nil {
					t.Errorf("unsubscribe failed: %v", err)
					return
				}
			}
		}()
	}

	finished := make(chan struct{})
	go func() {
		wg.Wait()
		close(finished)
	}()

	select {
	case <-finished:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for concurrent subscribe/unsubscribe/publish")
	}

	cancel()
	select {
	case err := <-runErr:
		if err != nil && !errors.Is(err, ErrNoSubscribers) {
			t.Fatalf("run returned unexpected error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for queue run to stop")
	}

	_ = received.Load()
}

func TestSubscribeUnsubscribePublish_ConcurrentClose(t *testing.T) {
	t.Parallel()

	q := NewQueue(QueueOptions{BufferSize: 4096})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	runErr := make(chan error, 1)
	go func() {
		runErr <- q.Run(ctx)
	}()

	n := atomic.Int64{}
	handler := func(Event) {
		n.Add(1)
	}
	q.Subscribe(queueTestEvent{}, handler)

	go func() {
		for {
			err := q.Publish(queueTestEvent{ID: 1})
			if err != nil && !errors.Is(err, ErrQueueClosed) {
				t.Errorf("publish expected to fail with ErrQueueClosed, got %v", err)
				return
			}
		}
	}()

	time.Sleep(10 * time.Millisecond)

	if err := q.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	select {
	case err := <-runErr:
		if err != nil && !errors.Is(err, ErrNoSubscribers) {
			t.Fatalf("run returned unexpected error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for queue run to stop")
	}

	if n.Load() == 0 {
		t.Fatal("expected at least one event to be delivered before close, got 0")
	}
}

func TestPublish_AfterCloseError(t *testing.T) {
	t.Parallel()

	q := NewQueue()
	if err := q.Subscribe(queueTestEvent{}, func(Event) {}); err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}
	if err := q.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("expected no panic when publishing after close, got %v", r)
		}
	}()

	if err := q.Publish(queueTestEvent{ID: 1}); err == nil || !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("expected ErrQueueClosed when publishing after close, got %v", err)
	}
}

func BenchmarkPublish(b *testing.B) {
	for _, n := range []int{1_000, 10_000, 100_000} {
		b.Run("subscribers="+strconv.Itoa(n), func(b *testing.B) {
			q := NewQueue(QueueOptions{BufferSize: 1024})
			ctx, cancel := context.WithCancel(b.Context())
			defer cancel()

			runErr := make(chan error, 1)
			go func() {
				runErr <- q.Run(ctx)
			}()

			var c atomic.Int64
			for range n {
				if err := q.Subscribe(queueTestEvent{}, func(Event) {
					c.Add(1)
				}); err != nil {
					b.Fatalf("subscribe failed: %v", err)
				}
			}

			b.ResetTimer()
			for b.Loop() {
				for {
					err := q.Publish(queueTestEvent{})
					if err == nil {
						break
					}
					if errors.Is(err, ErrQueueFull) {
						runtime.Gosched()
						continue
					}
					b.Fatalf("publish failed: %v", err)
				}
			}
			b.StopTimer()

			want := int64(b.N * n)
			deadline := time.Now().Add(30 * time.Second)
			for c.Load() < want {
				if time.Now().After(deadline) {
					b.Fatalf("timed out waiting for deliveries: got %d want %d", c.Load(), want)
				}
				runtime.Gosched()
			}

			cancel()
			if err := <-runErr; err != nil {
				b.Fatalf("run failed: %v", err)
			}

			if got := c.Load(); got != want {
				b.Fatalf("expected %d events, got %d", want, got)
			}
		})
	}
}

func BenchmarkPublishConcurrent(b *testing.B) {
	for _, workers := range []int{2, 4, 8, 16} {
		b.Run("workers="+strconv.Itoa(workers), func(b *testing.B) {
			q := NewQueue(QueueOptions{BufferSize: 1024})
			ctx, cancel := context.WithCancel(b.Context())
			defer cancel()

			runErr := make(chan error, 1)
			go func() {
				runErr <- q.Run(ctx)
			}()

			var delivered atomic.Int64
			if err := q.Subscribe(queueTestEvent{}, func(Event) {
				delivered.Add(1)
			}); err != nil {
				b.Fatalf("subscribe failed: %v", err)
			}

			b.ResetTimer()
			wgPub := sync.WaitGroup{}
			errCh := make(chan error, workers)
			for i := 0; i < workers; i++ {
				start := i * b.N / workers
				end := (i + 1) * b.N / workers
				wgPub.Add(1)
				go func(start, end int) {
					defer wgPub.Done()
					for j := start; j < end; j++ {
						for {
							err := q.Publish(queueTestEvent{})
							if err == nil {
								break
							}
							if errors.Is(err, ErrQueueFull) {
								runtime.Gosched()
								continue
							}
							errCh <- err
							return
						}
					}
				}(start, end)
			}
			wgPub.Wait()
			close(errCh)
			b.StopTimer()

			for err := range errCh {
				if err != nil {
					b.Fatalf("publish failed: %v", err)
				}
			}

			want := int64(b.N)
			deadline := time.Now().Add(30 * time.Second)
			for delivered.Load() < want {
				if time.Now().After(deadline) {
					b.Fatalf(
						"timed out waiting for deliveries: got %d want %d",
						delivered.Load(),
						want,
					)
				}
				runtime.Gosched()
			}

			cancel()
			if err := <-runErr; err != nil {
				b.Fatalf("run failed: %v", err)
			}

			if got := delivered.Load(); got != want {
				b.Fatalf("expected %d events, got %d", want, got)
			}
		})
	}
}
