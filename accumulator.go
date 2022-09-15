package accumulator

import (
	"sync"
	"sync/atomic"
	"time"
)

//Accumulator is a concurrency-safe "debounce" collection that starts timer after first enqueued item and collects until
//time is out and then sends slice of enqueued items to out channel
type Accumulator[T any] struct {
	out          chan []T
	storage      []T
	timer        *time.Timer
	timerStarted int32
	delay        time.Duration
	mux          sync.Mutex
	running      int32
	cap          int32
}

func NewAccumulator[T any](delay time.Duration, cap int32) *Accumulator[T] {
	timer := time.NewTimer(delay)
	timer.Stop()
	a := &Accumulator[T]{
		out:     make(chan []T),
		delay:   delay,
		timer:   timer,
		running: 1,
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
		default:
			if atomic.LoadInt32(&a.running) == 0 {
				close(a.out)
				return
			}
		}
	}
}

func (a *Accumulator[T]) Stop() {
	atomic.StoreInt32(&a.running, 0)
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
	a.timerStarted = 0
}

func (a *Accumulator[T]) Enqueue(t T) {
	if atomic.LoadInt32(&a.running) == 0 {
		panic("accumulator: Enqueue called on uninitialized Accumulator")
	}
	limit := atomic.LoadInt32(&a.cap)
	l := int32(len(a.storage))
	if limit > 0 && l == limit {
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
			default:
				if atomic.LoadInt32(&a.running) == 0 {
					return
				}
			}
		}
	}()
}
