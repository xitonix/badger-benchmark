package common

import (
	"sync"
	"sync/atomic"
	"time"
)

// Rater is a thread-safe rate counter
type Rater struct {
	counter   uint64
	startTime time.Time
	in        chan uint64
	wg        sync.WaitGroup
	interval  uint64
}

func NewRater(interval time.Duration) *Rater {
	return &Rater{
		interval: uint64(interval.Nanoseconds()),
	}
}

func (r *Rater) Start() {
	r.startTime = time.Now()
	r.in = make(chan uint64)
	r.counter = 0
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		for i := range r.in {
			atomic.AddUint64(&r.counter, i)
		}
	}()
}

func (r *Rater) Stop() (time.Duration, uint64) {
	stopTime := time.Now()
	close(r.in)
	r.wg.Wait()
	return r.averagePer(stopTime)
}

func (r *Rater) IncBy(i uint64) {
	r.in <- i
}

func (r *Rater) Inc() {
	r.IncBy(1)
}

func (r *Rater) Rate() uint64 {
	_, rate := r.averagePer(time.Now())
	return rate
}

func (r *Rater) averagePer(stopTime time.Time) (time.Duration, uint64) {
	c := atomic.LoadUint64(&r.counter)
	total := stopTime.Sub(r.startTime)
	totalNS := uint64(total.Nanoseconds())
	return total, (r.interval * c) / totalNS
}
