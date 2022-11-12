package utils

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func RandString(len int) string {
	bytes := make([]byte, len)
	for i := 0; i < len; i++ {
		b := r.Intn(26) + 65
		bytes[i] = byte(b)
	}
	return string(bytes)
}

func TestSkipListBasicCRUD(t *testing.T) {
	list := NewSkipList(1000)

	//Put & Get
	entry1 := NewEntry([]byte(RandString(10)), []byte("Val1"))
	list.Add(entry1)
	vs := list.Search(entry1.Key)
	assert.Equal(t, entry1.Value, vs.Value)

	entry2 := NewEntry([]byte(RandString(10)), []byte("Val2"))
	list.Add(entry2)
	vs = list.Search(entry2.Key)
	assert.Equal(t, entry2.Value, vs.Value)

	//Get a not exist entry
	assert.Nil(t, list.Search([]byte(RandString(10))).Value)

	//Update a entry
	entry2_new := NewEntry(entry1.Key, []byte("Val1+1"))
	list.Add(entry2_new)
	assert.Equal(t, entry2_new.Value, list.Search(entry2_new.Key).Value)
}

//当申请的zrenaSize为1000， 但是add的操作次数为1000000时
//会发生panic，why？
//原因：不断向skiplist插入数据，而arena不断申请内存
//当申请的内存超过预设的大小时，缺没有flush到sst的操作，所以会出错
//关于SST的相关知识，在lession5有进行讲解
func Benchmark_SkipListBasicCRUD(b *testing.B) {
	list := NewSkipList(100000000)
	key, val := "", ""
	maxTime := 1000
	for i := 0; i < maxTime; i++ {
		//number := rand.Intn(10000)
		key, val = RandString(10), fmt.Sprintf("Val%d", i)
		entry := NewEntry([]byte(key), []byte(val))
		list.Add(entry)
		searchVal := list.Search([]byte(key))
		assert.Equal(b, searchVal.Value, []byte(val))
	}
}

func TestConcurrentBasic(t *testing.T) {
	const n = 1000
	l := NewSkipList(100000000)
	var wg sync.WaitGroup
	key := func(i int) []byte {
		return []byte(fmt.Sprintf("Keykeykey%05d", i))
	}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			l.Add(NewEntry(key(i), key(i)))
		}(i)
	}
	wg.Wait()

	// Check values. Concurrent reads.
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v := l.Search(key(i))
			require.EqualValues(t, key(i), v.Value)
			return
		}(i)
	}
	wg.Wait()
}

func Benchmark_ConcurrentBasic(b *testing.B) {
	const n = 1000
	l := NewSkipList(100000000)
	var wg sync.WaitGroup
	key := func(i int) []byte {
		return []byte(fmt.Sprintf("keykeykey%05d", i))
	}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			l.Add(NewEntry(key(i), key(i)))
		}(i)
	}
	wg.Wait()

	// Check values. Concurrent reads.
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v := l.Search(key(i))
			require.EqualValues(b, key(i), v.Value)
			require.NotNil(b, v)
		}(i)
	}
	wg.Wait()
}
