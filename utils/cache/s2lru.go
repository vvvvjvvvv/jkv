package cache

import (
	"container/list"
	"fmt"
)

type segmentedLRU struct {
	data        map[uint64]*list.Element
	stageOneCap int
	stageOne    *list.List
	stageTwoCap int
	stageTwo    *list.List
}

const (
	STAGE_ONE = iota + 1
	STAGE_TWO
)

func newSLRU(data map[uint64]*list.Element, stageOneCap, stageTwoCap int) *segmentedLRU {
	return &segmentedLRU{
		data:        data,
		stageOneCap: stageOneCap,
		stageOne:    list.New(),
		stageTwoCap: stageTwoCap,
		stageTwo:    list.New(),
	}
}

func (slru *segmentedLRU) add(newItem storeItem) {
	// 进来都放 stageOne
	newItem.stage = 1

	// 如果 stageOne 没满，整个区域也没满
	if slru.stageOne.Len() < slru.stageOneCap || slru.Len() < slru.stageOneCap+slru.stageTwoCap {
		slru.data[newItem.key] = slru.stageOne.PushFront(&newItem)
		return
	}

	// stageOne 满了，或者整个都满了
	// 需要从 stageOne 中淘汰数据
	e := slru.stageOne.Back()
	item := e.Value.(*storeItem)

	delete(slru.data, item.key)

	*item = newItem

	slru.data[item.key] = e
	slru.stageOne.PushFront(e)
}

func (slru *segmentedLRU) get(v *list.Element) {
	item := v.Value.(*storeItem)

	// 若要访问的缓存数据，已经在 stageTwo 中，只需要按照 LRU 规则提前即可
	if item.stage == STAGE_TWO {
		slru.stageTwo.MoveToFront(v)
		return
	}

	// 若要访问的数据还在 stageOne 中，那么再次被访问到，就要升级到 stageTwo 阶段了
	if slru.stageTwo.Len() < slru.stageTwoCap {
		slru.stageOne.Remove(v)
		item.stage = STAGE_TWO
		slru.data[item.key] = slru.stageTwo.PushFront(item)
		return
	}

	// 新数据加入 stageTwo 需要淘汰旧数据
	// stageTwo 中淘汰的数据不会消失，会进入 stageOne 中
	// stageOne 中，访问频次更低的数据，有可能会被淘汰
	back := slru.stageTwo.Back()
	bItem := back.Value.(*storeItem)

	*bItem, *item = *item, *bItem

	bItem.stage = STAGE_TWO
	item.stage = STAGE_ONE

	slru.data[item.key] = v
	slru.data[bItem.key] = back

	slru.stageOne.MoveToFront(v)
	slru.stageTwo.MoveToFront(back)
}

func (slru *segmentedLRU) Len() int {
	return slru.stageOne.Len() + slru.stageTwo.Len()
}

func (slru *segmentedLRU) victim() *storeItem {
	// 如果 slru 的容量未满，不需要淘汰
	if slru.Len() < slru.stageOneCap+slru.stageTwoCap {
		return nil
	}

	// 如果已经满了，则需要从 20% 的区域淘汰数据，这里直接从尾部拿最后一个即可
	v := slru.stageOne.Back()
	return v.Value.(*storeItem)
}

func (slru *segmentedLRU) String() string {
	var s string
	for e := slru.stageTwo.Front(); e != nil; e = e.Next() {
		s += fmt.Sprintf("%v", e.Value.(*storeItem).value)
	}
	s += fmt.Sprintf(" | ")
	for e := slru.stageTwo.Front(); e != nil; e = e.Next() {
		s += fmt.Sprintf("%v", e.Value.(*storeItem).value)
	}
	return s
}
