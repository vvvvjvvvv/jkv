package cache

import (
	"container/list"
	"sync"
	"unsafe"

	"github.com/cespare/xxhash"
)

type Cache struct {
	m         sync.RWMutex
	lru       *windowLRU // 防止稀疏流量
	slru      *segmentedLRU
	door      *BloomFilter // 拒绝访问一次的数据
	c         *cmSketch    // 大概的频率统计，省内存空间
	t         int32        // 统计总共的访问次数
	threshold int32        // 数据保鲜的阈值
	data      map[uint64]*list.Element
}

type Options struct {
	lruPct uint8
}

func NewCache(size int) *Cache {
	// 定义 window 部分缓存所占比例，这里定义为 1%
	const lruPct = 1

	// 计算出来 window 部分的容量
	lruSz := size * lruPct / 100
	if lruSz < 1 {
		lruSz = 1
	}

	// 计算 lfu 部分的容量
	slruSz := int(float64(size) * ((100 - lruPct) / 100.0))
	if slruSz < 1 {
		slruSz = 1
	}

	// lfu 分为两部分，stageOne 的 probation 占20%
	slruO := int(0.2 * float64(slruSz))
	if slruO < 1 {
		slruO = 1
	}

	data := make(map[uint64]*list.Element, size)

	return &Cache{
		lru:  newWindowLRU(lruSz, data),
		slru: newSLRU(data, slruO, slruSz-slruO),
		door: newFilter(size, 0.01), // 布隆过滤器设置误差率为0.01
		c:    newCmSketch(int64(size)),
		data: data, // 共用同一个 map 存储数据
	}
}

func (c *Cache) Set(key, value interface{}) bool {
	c.m.Lock()
	defer c.m.Unlock()
	return c.set(key, value)
}

func (c *Cache) set(key, value interface{}) bool {
	if key == nil {
		return false
	}

	// keyHash 用来快速定位，conflict 用来判断冲突
	keyHash, conflictHash := c.keyToHash(key)

	// 刚放进去的缓存都先放到 window lru 中，所以 stage 置 0
	i := storeItem{
		stage:    0,
		key:      keyHash,
		conflict: conflictHash,
		value:    value,
	}

	// 如果 windows 已满，要反悔被淘汰的数据
	eItem, evited := c.lru.add(i)
	if evited == false {
		return true
	}

	// 如果 window 中有淘汰的数据，会走到这里
	// 需要从 LFU 的 stageOne 部分找到一个淘汰者
	// 二者进行 皇城pk
	victim := c.slru.victim()
	if victim == nil { // lfu 未满
		c.slru.add(eItem)
		return true
	}

	// 这里进行 PK， 必须在 bloomFilter 中至少出现过一次，才允许 pk
	if !c.door.Allow(uint32(eItem.key)) {
		return true
	}

	// 估算 wlru 和 lfu 中淘汰数据，历史访问次数
	// 访问次数越多，被认为越有资格留下来
	vCount := c.c.Estimate(victim.key)
	oCount := c.c.Estimate(eItem.key)
	if vCount < oCount {
		return true
	}

	c.slru.add(eItem)
	return true
}

func (c *Cache) Get(key interface{}) (interface{}, bool) {
	c.m.RLock()
	defer c.m.RUnlock()
	return c.get(key)
}

func (c *Cache) get(key interface{}) (interface{}, bool) {
	c.t++
	if c.t == c.threshold {
		c.c.Reset()
		c.door.reset()
		c.t = 0
	}

	keyHash, conflictHash := c.keyToHash(key)

	val, ok := c.data[keyHash]
	if !ok {
		c.door.Allow(uint32(keyHash))
		c.c.Increment(keyHash)
		return nil, false
	}

	item := val.Value.(*storeItem)
	if item.conflict != conflictHash {
		c.door.Allow(uint32(keyHash))
		c.c.Increment(keyHash)
		return nil, false
	}
	c.door.Allow(uint32(keyHash))
	c.c.Increment(item.key)

	v := item.value

	if item.stage == 0 {
		c.lru.get(val)
	} else {
		c.slru.get(val)
	}

	return v, true
}

func (c *Cache) Del(key interface{}) (interface{}, bool) {
	c.m.Lock()
	defer c.m.Unlock()
	return c.del(key)
}

func (c *Cache) del(key interface{}) (interface{}, bool) {
	keyHash, conflictHash := c.keyToHash(key)

	val, ok := c.data[keyHash]
	if !ok {
		return 0, false
	}

	item := val.Value.(*storeItem)
	if item.conflict != conflictHash {
		return 0, false
	}

	delete(c.data, keyHash)

	return item.conflict, true
}

func (c *Cache) keyToHash(key interface{}) (uint64, uint64) {
	if key == nil {
		return 0, 0
	}
	switch k := key.(type) {
	case uint64:
		return k, 0
	case string:
		return MemHashString(k), xxhash.Sum64String(k)
	case []byte:
		return MemHash(k), xxhash.Sum64(k)
	case byte:
		return uint64(k), 0
	case int:
		return uint64(k), 0
	case int32:
		return uint64(k), 0
	case uint32:
		return uint64(k), 0
	case int64:
		return uint64(k), 0
	default:
		panic("Key type not supported")
	}
}

type stringStruct struct {
	str unsafe.Pointer
	len int
}

//go:noescape
//go:linkname memhash runtime.memhash
func memhash(p unsafe.Pointer, h, s uintptr) uintptr

// MemHashString is the hash function used by go map, it utilizes available hardware instructions
// (behaves as aeshash if aes instruction is available).
// NOTE: The hash seed changes for every process. So, this cannot be used as a persistent hash.
func MemHashString(str string) uint64 {
	ss := (*stringStruct)(unsafe.Pointer(&str))
	return uint64(memhash(ss.str, 0, uintptr(ss.len)))
}

func MemHash(data []byte) uint64 {
	ss := (*stringStruct)(unsafe.Pointer(&data))
	return uint64(memhash(ss.str, 0, uintptr(ss.len)))
}

func (c *Cache) String() string {
	var s string
	s += c.lru.String() + " | " + c.slru.String()
	return s
}
