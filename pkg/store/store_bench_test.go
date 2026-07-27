package store

import (
	"fmt"
	"strconv"
	"testing"
)

func BenchmarkLPush(b *testing.B) {
	for _, numValues := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("Values_%d", numValues), func(b *testing.B) {
			values := make([]string, numValues)
			for i := 0; i < numValues; i++ {
				values[i] = "val"
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s := New()
				s.LPush("key", values)
			}
		})
	}
}

func BenchmarkLPush_Accumulate(b *testing.B) {
	values := make([]string, 10)
	for i := 0; i < 10; i++ {
		values[i] = "val"
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s := New()
		for j := 0; j < 100; j++ {
			s.LPush("key", values)
		}
	}
}

func BenchmarkDelMulti(b *testing.B) {
	s := New()
	keys := make([]string, 100)
	for i := 0; i < 100; i++ {
		keys[i] = "key" + strconv.Itoa(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Mock testing how it would look to lock once
		s.mu.Lock()
		count := 0
		for _, key := range keys {
			if _, ok := s.data[key]; ok {
				delete(s.data, key)
				count++
			}
		}
		s.mu.Unlock()
	}
}

func BenchmarkDelLoop(b *testing.B) {
	s := New()
	keys := make([]string, 100)
	for i := 0; i < 100; i++ {
		keys[i] = "key" + strconv.Itoa(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, k := range keys {
			s.Del(k)
		}
	}
}
