package store

import (
	"strconv"
	"testing"
)

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
