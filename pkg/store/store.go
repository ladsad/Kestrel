package store

import (
	"sort"
	"sync"
)

type Store struct {
	mu   sync.RWMutex
	data map[string]interface{}
}

func New() *Store {
	return &Store{
		data: make(map[string]interface{}),
	}
}

func (s *Store) Pause() {
	s.mu.Lock()
}

func (s *Store) Resume() {
	s.mu.Unlock()
}

func (s *Store) Del(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.data[key]; ok {
		delete(s.data, key)
		return 1
	}
	return 0
}

// Strings
func (s *Store) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, ok := s.data[key]
	if !ok {
		return "", false
	}
	str, ok := val.(string)
	return str, ok
}

// Hashes
func (s *Store) HSet(key, field, value string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.data[key]; !ok {
		s.data[key] = make(map[string]string)
	}
	hash, ok := s.data[key].(map[string]string)
	if !ok {
		return 0 // Wrong type
	}
	_, exists := hash[field]
	hash[field] = value
	if exists {
		return 0
	}
	return 1
}

func (s *Store) HGet(key, field string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, ok := s.data[key]
	if !ok {
		return "", false
	}
	hash, ok := val.(map[string]string)
	if !ok {
		return "", false
	}
	fieldVal, exists := hash[field]
	return fieldVal, exists
}

func (s *Store) HGetAll(key string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, ok := s.data[key]
	if !ok {
		return nil
	}
	hash, ok := val.(map[string]string)
	if !ok {
		return nil
	}
	var res []string
	for k, v := range hash {
		res = append(res, k, v)
	}
	return res
}

// Lists
func (s *Store) LPush(key string, values []string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.data[key]; !ok {
		s.data[key] = []string{}
	}
	list, ok := s.data[key].([]string)
	if !ok {
		return 0 // Wrong type
	}

	// Prepend
	for _, v := range values {
		list = append([]string{v}, list...)
	}
	s.data[key] = list
	return len(list)
}

func (s *Store) RPush(key string, values []string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.data[key]; !ok {
		s.data[key] = []string{}
	}
	list, ok := s.data[key].([]string)
	if !ok {
		return 0 // Wrong type
	}
	list = append(list, values...)
	s.data[key] = list
	return len(list)
}

func (s *Store) LPop(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	val, ok := s.data[key]
	if !ok {
		return "", false
	}
	list, ok := val.([]string)
	if !ok || len(list) == 0 {
		return "", false
	}
	head := list[0]
	s.data[key] = list[1:]
	return head, true
}

func (s *Store) RPop(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	val, ok := s.data[key]
	if !ok {
		return "", false
	}
	list, ok := val.([]string)
	if !ok || len(list) == 0 {
		return "", false
	}
	tail := list[len(list)-1]
	s.data[key] = list[:len(list)-1]
	return tail, true
}

// Sets
func (s *Store) SAdd(key string, members []string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.data[key]; !ok {
		s.data[key] = make(map[string]struct{})
	}
	set, ok := s.data[key].(map[string]struct{})
	if !ok {
		return 0 // Wrong type
	}
	added := 0
	for _, m := range members {
		if _, exists := set[m]; !exists {
			set[m] = struct{}{}
			added++
		}
	}
	return added
}

func (s *Store) SMembers(key string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, ok := s.data[key]
	if !ok {
		return nil
	}
	set, ok := val.(map[string]struct{})
	if !ok {
		return nil
	}
	var res []string
	for k := range set {
		res = append(res, k)
	}
	return res
}

func (s *Store) SIsMember(key, member string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, ok := s.data[key]
	if !ok {
		return 0
	}
	set, ok := val.(map[string]struct{})
	if !ok {
		return 0
	}
	if _, exists := set[member]; exists {
		return 1
	}
	return 0
}

// Sorted Sets (Basic implementation)
type ZSetMember struct {
	Score  float64
	Member string
}

type ZSet struct {
	dict map[string]float64
}

func (s *Store) ZAdd(key string, score float64, member string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.data[key]; !ok {
		s.data[key] = &ZSet{dict: make(map[string]float64)}
	}
	zset, ok := s.data[key].(*ZSet)
	if !ok {
		return 0 // Wrong type
	}
	_, exists := zset.dict[member]
	zset.dict[member] = score
	if exists {
		return 0
	}
	return 1
}

func (s *Store) ZRange(key string, start, stop int) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, ok := s.data[key]
	if !ok {
		return nil
	}
	zset, ok := val.(*ZSet)
	if !ok {
		return nil
	}
	var members []ZSetMember
	for m, sc := range zset.dict {
		members = append(members, ZSetMember{Score: sc, Member: m})
	}
	sort.Slice(members, func(i, j int) bool {
		if members[i].Score == members[j].Score {
			return members[i].Member < members[j].Member
		}
		return members[i].Score < members[j].Score
	})

	length := len(members)
	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= length {
		stop = length - 1
	}
	if start > stop || start >= length {
		return nil
	}

	var res []string
	for i := start; i <= stop; i++ {
		res = append(res, members[i].Member)
	}
	return res
}
