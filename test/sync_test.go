package test

import (
	"regexp"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRegex(t *testing.T) {
	p := regexp.MustCompile("abc(def)")
	if p.NumSubexp() != 1 {
		t.Fail()
	}
}

//BenchmarkTimeNow time.now
func BenchmarkTimeNow(b *testing.B) {
	// run the Fib function b.N times

	for n := 0; n < b.N; n++ {
		_ = time.Now()
	}
}

//BenchmarkAtomicLoad time.now
func BenchmarkAtomicLoad(b *testing.B) {
	// run the Fib function b.N times
	var t atomic.Value
	t.Store("test string")

	for n := 0; n < b.N; n++ {
		_ = t.Load()
	}
}

//BenchmarkTimeNow time.now
func BenchmarkMutex(b *testing.B) {
	// run the Fib function b.N times
	var t sync.Mutex

	for n := 0; n < b.N; n++ {
		t.Lock()
		t.Unlock()
	}
}
