package utils

import (
	"bytes"
	"sync"
)

const (
	defaultMaxLevel = 48
)

type SkipList struct {
	maxLevel   int          // sl的最大高度
	lock       sync.RWMutex // 读写锁，用来实现并发安全的sl
	currHeight int32        // sl当钱最大高度
	headOffset uint32       // 头节点在arena中的位置
	arena      *Arena
}

func NewSkipList(arenaSize int64) *SkipList {
	arena := newArena(arenaSize)
	// 引入一个空的头节点，因此key和value都是空的
	head := newElement(arena, nil, ValueStruct{}, defaultMaxLevel)
	ho := arena.getElementOffset(head)

	return &SkipList{
		currHeight: 1,
		headOffset: ho,
		arena:      arena,
	}
}

type Element struct {
	score     float64 // 加快查找，只在内存中生效，因此不需要持久化
	value     uint64  // 将value的off和size组装成一个uint64，实现原子化的操作
	keyOffset uint32
	keySize   uint16
	height    uint16
	levels    [defaultMaxLevel]uint32 // 这里先按最大高度申明，往arena中放置的时候，会计算实际高度和内存消耗
}

func encodeValue(valOffset uint32, valSize uint32) uint64 {
	return uint64(valSize)<<32 | uint64(valOffset)
}

func decodeValue(value uint64) (valOffset, valSize uint32) {
	valOffset = uint32(value)
	valSize = uint32(value >> 32)
	return
}

func newElement(arena *Arena, key []byte, v ValueStruct, height int) *Element {
	nodeOffset := arena.putNode(height)
	keyOffset := arena.putKey(key)
	val := encodeValue(arena.putVal(v), v.EncodedSize())
	elem := arena.getElement(nodeOffset)
	elem.score = calcScore(key)
	elem.keyOffset = keyOffset
	elem.keySize = uint16(len(key))
	elem.height = uint16(height)
	elem.value = val

	return elem
}

func (e *Element) key(arena *Arena) []byte {
	return arena.getKey(e.keyOffset, e.keySize)
}

func (list *SkipList) Size() int64 {
	return list.arena.Size()
}

func (list *SkipList) Add(data *Entry) error {
	list.lock.Lock()
	defer list.lock.Unlock()
	score := calcScore(data.Key)
	var elem *Element
	value := ValueStruct{
		Value: data.Value,
	}

	// 从当前最大高度开始
	max := list.currHeight
	// 拿到头节点，从第一个开始
	prevElem := list.arena.getElement(list.headOffset)
	// 用来记录访问路径
	var prevElemHeaders [defaultMaxLevel]*Element

	for i := max - 1; i >= 0; {
		prevElemHeaders[i] = prevElem

		for next := list.getNext(prevElem, int(i)); next != nil; next = list.getNext(prevElem, int(i)) {
			if comp := list.compare(score, data.Key, next); comp <= 0 {
				if comp == 0 {
					vo := list.arena.putVal(value)
					encV := encodeValue(vo, value.EncodedSize())
					next.value = encV
					return nil
				}

				break
			}
			prevElem = next
			prevElemHeaders[i] = prevElem
		}

		topLevel := prevElem.levels[i]

		for i--; i >= 0 && prevElem.levels[i] == topLevel; i-- {
			prevElemHeaders[i] = prevElem
		}
	}

	level := list.randLevel()

	elem = newElement(list.arena, data.Key, ValueStruct{Value: data.Value}, level)

	off := list.arena.getElementOffset(elem)
	for i := 0; i < level; i++ {
		elem.levels[i] = prevElemHeaders[i].levels[i]
		prevElemHeaders[i].levels[i] = off
	}

	return nil
}

func (list *SkipList) Search(key []byte) (e *Entry) {
	list.lock.Lock()
	defer list.lock.Unlock()
	if list.arena.Size() == 0 {
		return nil
	}

	score := calcScore(key)

	prevElem := list.arena.getElement(list.headOffset)
	i := list.currHeight

	for i >= 0 {
		for next := list.getNext(prevElem, int(i)); next != nil; next = list.getNext(prevElem, int(i)) {
			if comp := list.compare(score, key, next); comp <= 0 {
				if comp == 0 {
					vo, vs := decodeValue(next.value)
					return &Entry{Key: key, Value: list.arena.getVal(vo, vs).Value}
				}
				break
			}

			prevElem = next
		}

		topLevel := prevElem.levels[i]

		for i--; i >= 0 && prevElem.levels[i] == topLevel; i-- {

		}
	}

	return
}

func (list *SkipList) Close() error {
	return nil
}

func calcScore(key []byte) (score float64) {
	var hash uint64
	l := len(key)

	if l > 8 {
		l = 8
	}

	for i := 0; i < l; i++ {
		shift := uint(64 - 8 - i*8)
		hash |= uint64(key[i]) << shift
	}

	score = float64(hash)
	return
}

func (list *SkipList) compare(score float64, key []byte, next *Element) int {
	if score == next.score {
		return bytes.Compare(key, next.key(list.arena))
	}

	if score < next.score {
		return -1
	} else {
		return 1
	}
}

func (list *SkipList) randLevel() int {
	if list.maxLevel <= 1 {
		return 1
	}

	i := 1
	for ; i < list.maxLevel; i++ {
		if RandN(1000)%2 == 0 {
			return i
		}
	}

	return i
}

// 拿到某个节点，在某个高度上的next节点
// 如果该节点已经是最后一个节点（该节点的level[height]将会是0，会返回nil
func (list *SkipList) getNext(e *Element, height int) *Element {
	return list.arena.getElement(e.getNextOffset(height))
}

type SkipListIter struct {
	list *SkipList
	elem *Element // iterator当前持有的节点
	lock sync.RWMutex
}

func (list *SkipList) NewSkipListIterator() Iterator {
	return &SkipListIter{}
}

func (iter *SkipListIter) Next() {
	AssertTrue(iter.Valid())
	iter.elem = iter.list.getNext(iter.elem, 0) // 只在最底层遍历
}

func (iter *SkipListIter) Valid() bool {
	return iter.elem != nil
}

func (iter *SkipListIter) Rewind() {
	head := iter.list.arena.getElement(iter.list.headOffset)
	iter.elem = iter.list.getNext(head, 0)
}

func (iter *SkipListIter) Item() Item {
	vo, vs := decodeValue(iter.elem.value)
	return &Entry{
		Key:       iter.list.arena.getKey(iter.elem.keyOffset, iter.elem.keySize),
		Value:     iter.list.arena.getVal(vo, vs).Value,
		ExpiresAt: iter.list.arena.getVal(vo, vs).ExpiresAt,
	}
}

func (iter *SkipListIter) Close() error {
	return nil
}

func (iter *SkipListIter) Seek(key []byte) {

}
