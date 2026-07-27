package store

import (
	"strconv"
	"testing"
)

func BenchmarkHSetMulti(b *testing.B) {
	s := New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := "hkey"
		// add 100 fields using HSetMulti
		fields := make([]string, 100)
		vals := make([]string, 100)
		for j := 0; j < 100; j++ {
			fields[j] = "field" + strconv.Itoa(j)
			vals[j] = "val" + strconv.Itoa(j)
		}
		s.HSetMulti(key, fields, vals)
	}
}
