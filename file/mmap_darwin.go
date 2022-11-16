package file

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/vvvvjvvvv/jkv/utils/mmap"
)

type MmapFile struct {
	Data []byte
	Fd   *os.File
}

// OpenMmapFileUsing os
func OpenMmapFileUsing(fd *os.File, sz int, writable bool) (*MmapFile, error) {
	filename := fd.Name()
	fi, err := fd.Stat()
	if err != nil {
		return nil, errors.Wrapf(err, "cannot stat file: %s", filename)
	}

	var rerr error
	fileSize := fi.Size()
	if sz > 0 && fileSize == 0 {
		// if the file is empty, truncate it to sz
		if err := fd.Truncate(int64(sz)); err != nil {
			return nil, errors.Wrapf(err, "err while truncation")
		}
		fileSize = int64(sz)
	}

	// fmt.Printf("Mmaping file: %s with writable: %v filesize: %d\n", fd.Name(), writable, fileSize)
	buf, err := mmap.Mmap(fd, writable, fileSize)
	if err != nil {
		return nil, errors.Wrapf(err, "while mmapping %s with size: %d", fd.Name(), fileSize)
	}

	if fileSize == 0 { // sz == 0, no file created, force SyncDir
		dir, _ := filepath.Split(filename)
		go SyncDir(dir)
	}
	return &MmapFile{
		Data: buf,
		Fd:   fd,
	}, rerr
}

// OpenMmapFile opens an existing file or creates a new file. If the file is
// created, it would truncate the file to maxSz. In both cases, it would mmap
// the file to maxSz and returned it. In case the file is created, z.NewFile is
// returned
func OpenMmapFile(filename string, flag int, maxSz int) (*MmapFile, error) {
	// fmt.Printf("opening file %s with flag: %v\n", filename, flag)
	fd, err := os.OpenFile(filename, flag, 0666)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to open: %s", filename)
	}
	writable := true
	if flag == os.O_RDONLY {
		writable = false
	}
	return OpenMmapFileUsing(fd, maxSz, writable)
}

type mmapReader struct {
	Date   []byte
	offset int
}

func (mr *mmapReader) Read(buf []byte) (int, error) {
	if mr.offset > len(mr.Date) {
		return 0, io.EOF
	}

	n := copy(buf, mr.Date[mr.offset:])
	mr.offset = n
	if n < len(buf) {
		return n, io.EOF
	}

	return n, nil
}

func (m *MmapFile) NewReader(offset int) io.Reader {
	return &mmapReader{
		Date:   m.Data,
		offset: offset,
	}
}

// Bytes returns data starting from offset off of size sz. If there's not enough data, it would
// return nil slice and io.EOF.
func (m *MmapFile) Bytes(off, sz int) ([]byte, error) {
	if len(m.Data[off:]) < sz {
		return nil, io.EOF
	}
	return m.Data[off : off+sz], nil
}

const oneGB = 1 << 30

// AppendBuffer 向内存中追加一个buffer，如果空间不足则重新映射，扩大空间
func (m *MmapFile) AppendBuffer(offset uint32, buf []byte) error {
	size := len(m.Data)
	needSize := len(buf)
	end := int(offset) + needSize
	if end > size {
		growBy := size
		if growBy > oneGB {
			growBy = oneGB
		}
		if growBy < needSize {
			growBy = needSize
		}
		if err := m.Truncature(int64(end)); err != nil {
			return err
		}
	}
	dLen := copy(m.Data[offset:end], buf)
	if dLen != needSize {
		return errors.Errorf("dLen != needSize AppendBuffer failed")
	}
	return nil
}

func (m *MmapFile) Sync() error {
	if m == nil {
		return nil
	}
	return mmap.Msync(m.Data)
}

func (m *MmapFile) Delete() error {
	if m.Fd == nil {
		return nil
	}

	if err := mmap.Munmap(m.Data); err != nil {
		return fmt.Errorf("while munmap file: %s, error: %v\n", m.Fd.Name(), err)
	}
	m.Data = nil
	if err := m.Fd.Truncate(0); err != nil {
		return fmt.Errorf("while truncate file: %s, error: %v\n", m.Fd.Name(), err)
	}
	if err := m.Fd.Close(); err != nil {
		return fmt.Errorf("while close file: %s, error: %v\n", m.Fd.Name(), err)
	}
	return os.Remove(m.Fd.Name())
}

// Close would close the file. It would also truncate the file if maxSz >= 0
func (m *MmapFile) Close() error {
	if m.Fd == nil {
		return nil
	}
	if err := m.Sync(); err != nil {
		return fmt.Errorf("while sync file: %s, error: %v\n", m.Fd.Name(), err)
	}
	if err := mmap.Munmap(m.Data); err != nil {
		return fmt.Errorf("while munmap file: %s, error: %v\n", m.Fd.Name(), err)
	}
	return m.Fd.Close()
}

func SyncDir(dir string) error {
	df, err := os.Open(dir)
	if err != nil {
		return errors.Wrapf(err, "while opening: %s", dir)
	}

	if err := df.Sync(); err != nil {
		return errors.Wrapf(err, "while syncing: %s", dir)
	}

	if err := df.Close(); err != nil {
		return errors.Wrapf(err, "while closing: %s", dir)
	}

	return nil
}

// Truncature 兼容接口
func (m *MmapFile) Truncature(maxSz int64) error {
	if err := m.Sync(); err != nil {
		return fmt.Errorf("while sync file: %s, error: %v\n", m.Fd.Name(), err)
	}
	if err := mmap.Munmap(m.Data); err != nil {
		return fmt.Errorf("while munmap file: %s, error: %v\n", m.Fd.Name(), err)
	}
	if err := m.Fd.Truncate(maxSz); err != nil {
		return fmt.Errorf("while truncate file: %s, error: %v\n", m.Fd.Name(), err)
	}
	var err error
	m.Data, err = mmap.Mmap(m.Fd, true, maxSz) // Mmap up to max size.
	return err
}

// ReName 兼容接口
func (m *MmapFile) ReName(name string) error {
	return nil
}
