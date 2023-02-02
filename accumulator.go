package accumulator

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Accumulator is a concurrency-safe "debounce" collection that starts timer after first enqueued item and holds enqueued until
// time is out or the cap (limit) is reached and then sends slice of enqueued items to out channel. 0 cap is ignored
type Accumulator[T any] struct {
	out          chan []T
	storage      []T
	timer        *time.Timer
	timerStarted int32
	delay        time.Duration
	mux          sync.Mutex
	stopCtx      context.Context
	stopFn       context.CancelFunc
	cap          int
}

func NewAccumulator[T any](ctx context.Context, delay time.Duration, cap int) *Accumulator[T] {
	timer := time.NewTimer(delay)
	timer.Stop()
	stopCtx, stopFn := context.WithCancel(ctx)
	a := &Accumulator[T]{
		out:     make(chan []T),
		delay:   delay,
		timer:   timer,
		stopCtx: stopCtx,
		stopFn:  stopFn,
		cap:     cap,
	}
	go a.run()
	return a
}

func (a *Accumulator[T]) run() {
	for {
		select {
		case <-a.timer.C:
			a.fanOut()
		case <-a.stopCtx.Done():
			close(a.out)
			return
		}
	}
}

func (a *Accumulator[T]) Stop() {
	a.stopFn()
}

func (a *Accumulator[T]) fanOut() {
	a.mux.Lock()
	defer a.mux.Unlock()
	if len(a.storage) > 0 {
		var c = make([]T, len(a.storage), len(a.storage))
		copy(c, a.storage)
		a.storage = nil
		go func(c []T) {
			a.out <- c
		}(c)
	}
	atomic.StoreInt32(&a.timerStarted, 0)
}

func (a *Accumulator[T]) Enqueue(t T) {
	if a.stopCtx.Err() != nil {
		panic("accumulator: Enqueue called on uninitialized Accumulator")
	}
	if a.cap > 0 && len(a.storage) == a.cap {
		a.fanOut()
	}
	if atomic.CompareAndSwapInt32(&a.timerStarted, 0, 1) {
		a.timer.Reset(a.delay)
	}
	a.mux.Lock()
	defer a.mux.Unlock()
	a.storage = append(a.storage, t)
}

func (a *Accumulator[T]) Out() chan []T {
	return a.out
}

func (a *Accumulator[T]) OnReady(cb func([]T)) {
	go func() {
		for {
			select {
			case d := <-a.out:
				if d == nil {
					return
				}
				cb(d)
			case <-a.stopCtx.Done():
				return
			}
		}
	}()
}
