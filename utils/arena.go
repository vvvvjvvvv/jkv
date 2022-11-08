package utils

import (
	"log"
	"sync/atomic"
	"unsafe"

	"github.com/pkg/errors"
)

type Arena struct {
	n   uint32 // used, offset
	buf []byte // allocated by Arena
}

const (
	MaxNodeSize = int(unsafe.Sizeof(Element{}))
	offsetSize  = int(unsafe.Sizeof(uint32(0)))     // level数组中一个元素的大小，用于计算实际高度
	nodeAlign   = int(unsafe.Sizeof(uint64(0)) - 1) // 内存对齐的内存单元 - 1 用于内存对齐时向上取整
)

func newArena(n int64) *Arena {
	return &Arena{
		n:   1,
		buf: make([]byte, n),
	}
}

// 从arena中申请sz大小的空间，从offset - sz开始
func (s *Arena) allocate(sz uint32) uint32 {
	offset := atomic.AddUint32(&s.n, sz)

	// 如果要分配的空间不足以放下一个新节点
	if int(offset) > len(s.buf)-MaxNodeSize {
		// 把 arena 的空间 double 一下
		growBy := uint32(len(s.buf))
		if growBy > 1<<30 {
			growBy = 1 << 30
		}
		if growBy < sz {
			growBy = sz
		}

		newBuf := make([]byte, len(s.buf)+int(growBy))
		// rcu操作，全量copy到newBuf中，然后设置为新的arena内存池
		AssertTrue(len(s.buf) == copy(newBuf, s.buf))
		s.buf = newBuf
	}

	return offset - sz // 起始地址
}

// 在 arena 里开辟一块空间，用以存放skiplist中的节点
// 返回值为arena中的offset
func (s *Arena) putNode(height int) uint32 {
	unusedSize := (defaultMaxLevel - height) * offsetSize // level[]中没用上的空间
	l := uint32(MaxNodeSize - unusedSize + nodeAlign)
	n := s.allocate(l)
	m := (n + uint32(nodeAlign)) & ^uint32(nodeAlign)

	return m
}

func (s *Arena) putKey(key []byte) uint32 {
	keySz := uint32(len(key))
	offset := s.allocate(keySz)
	buf := s.buf[offset : offset+keySz]
	AssertTrue(len(key) == copy(buf, key))
	return offset
}

func (s *Arena) putVal(v ValueStruct) uint32 {
	l := v.EncodedSize()
	offset := s.allocate(l)
	v.EncodeValue(s.buf[offset:])
	return offset
}

func (s *Arena) getElement(offset uint32) *Element {
	if offset == 0 {
		return nil
	}

	return (*Element)(unsafe.Pointer(&s.buf[offset]))
}

func (s *Arena) getKey(offset uint32, size uint16) []byte {
	return s.buf[offset : offset+uint32(size)]
}

func (s *Arena) getVal(offset uint32, size uint32) (v ValueStruct) {
	v.DecodeValue(s.buf[offset : offset+size])
	return
}

func (s *Arena) getElementOffset(nd *Element) uint32 {
	if nd == nil {
		return 0
	}

	return uint32(uintptr(unsafe.Pointer(nd)) - uintptr(unsafe.Pointer(&s.buf[0])))
}

func (e *Element) getNextOffset(h int) uint32 {
	return atomic.LoadUint32(&e.levels[h])
}

func (s *Arena) Size() int64 {
	return int64(atomic.LoadUint32(&s.n))
}

func AssertTrue(b bool) {
	if !b {
		log.Fatalf("%+v", errors.Errorf("Assert failed"))
	}
}
