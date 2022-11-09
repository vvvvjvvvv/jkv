package cache

import (
	"container/list"
	"fmt"
)

type windowLRU struct {
	data map[uint64]*list.Element
	list *list.List
	cap  int
}

type storeItem struct {
	stage    int
	key      uint64
	conflict uint64 // 当 key 冲突的时候，辅助判断
	value    interface{}
}

func newWindowLRU(size int, data map[uint64]*list.Element) *windowLRU {
	return &windowLRU{
		data: data,
		list: list.New(),
		cap:  size,
	}
}

func (lru *windowLRU) add(newItem storeItem) (eItem storeItem, evicted bool) {
	// 如果 windowLRU 部分容量未满，直接插入
	if lru.list.Len() < lru.cap {
		lru.data[newItem.key] = lru.list.PushFront(&newItem)
		return storeItem{}, false
	}

	// 如果 windowLRU 部分容量已满，按照 lru 规则从尾部淘汰
	evictItem := lru.list.Back()
	item := evictItem.Value.(*storeItem)

	// 从 slice 中删除这条数据
	delete(lru.data, item.key)

	// 这里直接对 evictItem 和 *item 赋值，避免向 runtime 再次申请空间
	eItem, *item = *item, newItem

	lru.data[item.key] = evictItem
	lru.list.MoveToFront(evictItem)

	return eItem, true // 返回要淘汰的元素，是否有被淘汰的元素
}

func (lru *windowLRU) get(v *list.Element) {
	lru.list.MoveToFront(v)
}

func (lru *windowLRU) String() string {
	var s string
	for e := lru.list.Front(); e != nil; e = e.Next() {
		s += fmt.Sprintf("%v", e.Value.(*storeItem).value)
	}
	return s
}
