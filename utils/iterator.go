package utils

type Iterator interface {
	Next()
	Valid() bool
	Rewind()
	Item() Item
	Close() error
	Seek(key []byte)
}

type Item interface {
	Entry() *Entry
}

type Options struct {
	Prefix []byte // 指定的前缀
	IsAsc  bool   // 是否升序
}
