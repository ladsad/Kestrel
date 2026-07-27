package store

import (
	"fmt"
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
