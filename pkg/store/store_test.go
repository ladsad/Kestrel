package store

import (
	"strconv"
	"sync"
	"testing"
)

func TestStore_Strings(t *testing.T) {
	s := New()
	s.Set("key1", "val1")
	val, ok := s.Get("key1")
	if !ok || val != "val1" {
		t.Errorf("Expected val1, got %v", val)
	}

	s.Del("key1")
	_, ok = s.Get("key1")
	if ok {
		t.Errorf("Expected key1 to be deleted")
	}
}

func TestStore_Hashes(t *testing.T) {
	s := New()
	s.HSet("hkey", "f1", "v1")
	s.HSet("hkey", "f2", "v2")
	val, ok := s.HGet("hkey", "f1")
	if !ok || val != "v1" {
		t.Errorf("Expected v1, got %v", val)
	}
	all := s.HGetAll("hkey")
	if len(all) != 4 {
		t.Errorf("Expected length 4, got %d", len(all))
	}
}

func TestStore_Lists(t *testing.T) {
	s := New()
	s.LPush("lkey", []string{"a", "b"}) // list is b, a
	s.RPush("lkey", []string{"c", "d"}) // list is b, a, c, d
	
	val, ok := s.LPop("lkey")
	if !ok || val != "b" {
		t.Errorf("Expected b, got %v", val)
	}
	val, ok = s.RPop("lkey")
	if !ok || val != "d" {
		t.Errorf("Expected d, got %v", val)
	}
}

func TestStore_Sets(t *testing.T) {
	s := New()
	s.SAdd("skey", []string{"a", "b", "c", "a"})
	if s.SIsMember("skey", "a") != 1 {
		t.Errorf("Expected 'a' to be a member")
	}
	if s.SIsMember("skey", "d") != 0 {
		t.Errorf("Expected 'd' not to be a member")
	}
	members := s.SMembers("skey")
	if len(members) != 3 {
		t.Errorf("Expected 3 members, got %d", len(members))
	}
}

func TestStore_ZSets(t *testing.T) {
	s := New()
	s.ZAdd("zkey", 2.0, "b")
	s.ZAdd("zkey", 1.0, "a")
	s.ZAdd("zkey", 3.0, "c")
	
	rangeVals := s.ZRange("zkey", 0, -1)
	if len(rangeVals) != 3 || rangeVals[0] != "a" || rangeVals[1] != "b" || rangeVals[2] != "c" {
		t.Errorf("Expected [a b c], got %v", rangeVals)
	}
}

func TestStore_Concurrency(t *testing.T) {
	s := New()
	var wg sync.WaitGroup
	
	// Concurrent writes
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			s.Set("key"+strconv.Itoa(n), strconv.Itoa(n))
		}(i)
	}
	
	wg.Wait()
	
	// Concurrent reads
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			val, ok := s.Get("key" + strconv.Itoa(n))
			if !ok || val != strconv.Itoa(n) {
				t.Errorf("Concurrency failure on key%d", n)
			}
		}(i)
	}
	wg.Wait()
}
