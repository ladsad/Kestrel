package sharding

import (
	"hash/fnv"
	"sort"
	"strconv"
)

type HashRing struct {
	virtualNodes int
	keys         []uint32
	hashMap      map[uint32]string // Maps hash to Shard ID
}

func NewHashRing(virtualNodes int) *HashRing {
	return &HashRing{
		virtualNodes: virtualNodes,
		hashMap:      make(map[uint32]string),
	}
}

func (h *HashRing) AddNode(nodeID string) {
	for i := 0; i < h.virtualNodes; i++ {
		hash := h.hashKey(nodeID + ":" + strconv.Itoa(i))
		h.keys = append(h.keys, hash)
		h.hashMap[hash] = nodeID
	}
	sort.Slice(h.keys, func(i, j int) bool {
		return h.keys[i] < h.keys[j]
	})
}

func (h *HashRing) GetNode(key string) string {
	if len(h.keys) == 0 {
		return ""
	}

	hash := h.hashKey(key)

	// Binary search for appropriate replica.
	idx := sort.Search(len(h.keys), func(i int) bool {
		return h.keys[i] >= hash
	})

	// Means we have cycled back to the first replica.
	if idx == len(h.keys) {
		idx = 0
	}

	return h.hashMap[h.keys[idx]]
}

func (h *HashRing) hashKey(key string) uint32 {
	h32 := fnv.New32a()
	h32.Write([]byte(key))
	return h32.Sum32()
}
