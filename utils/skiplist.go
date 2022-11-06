package utils

import (
	"bytes"
	"math/rand"
	"sync"
	"time"

	"github.com/vvvvjvvvv/jkv/utils/codec"
)

const (
	defaultMaxLevel = 48
)

type SkipList struct {
	header *Element

	rand *rand.Rand

	maxLevel int
	length   int
	lock     sync.RWMutex
	size     int64
}

func NewSkipList() *SkipList {
	source := rand.NewSource(time.Now().UnixNano())

	return &SkipList{
		header: &Element{
			levels: make([]*Element, defaultMaxLevel),
			entry:  nil,
			score:  0,
		},
		rand:     rand.New(source),
		length:   0,
		maxLevel: defaultMaxLevel,
	}
}

type Element struct {
	levels []*Element
	entry  *codec.Entry
	score  float64
}

func newElement(score float64, entry *codec.Entry, level int) *Element {
	return &Element{
		levels: make([]*Element, level),
		entry:  entry,
		score:  score,
	}
}

func (elem *Element) Entry() *codec.Entry {
	return elem.entry
}

func (list *SkipList) Add(data *codec.Entry) error {
	list.lock.Lock()
	defer list.lock.Unlock()
	score := list.calcScore(data.Key)
	var elem *Element

	prevElem := list.header

	var prevElemHeaders [defaultMaxLevel]*Element

	for i := len(list.header.levels) - 1; i >= 0; {

		// 用于记录当前元素在每一层中插入位置的前一个元素，默认是header
		prevElemHeaders[i] = prevElem

		for next := prevElem.levels[i]; next != nil; next = prevElem.levels[i] {
			if comp := list.compare(score, data.Key, next); comp <= 0 {
				if comp == 0 { // key相同，直接覆盖掉val
					elem = next
					elem.entry = data
					list.size += elem.entry.Size() - data.Size()
					return nil
				}
				break
			}
			prevElem = next
			prevElemHeaders[i] = prevElem // 更新
		}

		topLevel := prevElem.levels[i]

		for i--; i >= 0 && prevElem.levels[i] == topLevel; i-- {
			prevElemHeaders[i] = prevElem // 更新跳过的case
		}
	}

	level := list.randLevel()

	elem = newElement(score, data, level)

	for i := 0; i < level; i++ {
		elem.levels[i] = prevElemHeaders[i].levels[i]
		prevElemHeaders[i].levels[i] = elem
	}
	list.size += data.Size()
	list.length++
	return nil
}

func (list *SkipList) Search(key []byte) (e *codec.Entry) {
	list.lock.RLock()
	defer list.lock.RUnlock()

	if list.length == 0 {
		return nil
	}

	score := list.calcScore(key)
	prevElem := list.header

	// 从顶层开始遍历
	for i := len(list.header.levels) - 1; i >= 0; {
		// 搭配循环体里的 prevElem = next
		for next := prevElem.levels[i]; next != nil; next = prevElem.levels[i] {
			// 小于：应该往下一层
			// 等于：那就是你了
			// 大于：接着遍历
			if comp := list.compare(score, key, next); comp <= 0 {
				if comp == 0 {
					return next.Entry()
				}
				break
			}
			prevElem = next
		}

		// 顶层大于党员元素的元素，为了跳过一些case
		topLevel := prevElem.levels[i]
		for i--; i >= 0 && prevElem.levels[i] == topLevel; i-- {

		}
	}

	return nil
}

func (list *SkipList) Close() error {
	return nil
}

func (list *SkipList) calcScore(key []byte) (score float64) {
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
		return bytes.Compare(key, next.entry.Key)
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

func (list *SkipList) Size() int64 {
	return list.size
}
