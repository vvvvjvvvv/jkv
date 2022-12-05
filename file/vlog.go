package file

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/vvvvjvvvv/jkv/utils"
)

type LogFile struct {
	Lock sync.RWMutex
	FID  uint32
	size uint32
	f    *MmapFile
}

func (lf *LogFile) Open(opt *Options) error {
	var err error
	lf.FID = uint32(opt.FID)
	lf.Lock = sync.RWMutex{}
	lf.f, err = OpenMmapFile(opt.FileName, os.O_CREATE|os.O_RDWR, opt.MaxSz)
	utils.Panic2(nil, err)
	fi, err := lf.f.Fd.Stat()
	if err != nil {
		return utils.WarpErr("Unable to run file.Stat", err)
	}

	// 获取文件尺寸
	sz := fi.Size()
	utils.CondPanic(sz > math.MaxUint32, fmt.Errorf("file size: %d greater than %d",
		uint32(sz), uint32(math.MaxUint32)))
	lf.size = uint32(sz)
	// TODO 是否需要在这里弄一个header放一些元素
	return nil
}

// Acquire lock on mmap/file if you are calling this
func (lf *LogFile) Read(p *utils.ValuePtr) (buf []byte, err error) {
	offset := p.Offset
	// Do not convert size to uint32, because the lf.fmap can be of size
	// 4GB, which overflows the uint32 during conversion to make the size 0,
	// causing the read to fail the ErrEOF.
	size := int64(len(lf.f.Data))
	valSz := p.Len
	lfSz := atomic.LoadUint32(&lf.size)
	if int64(offset) >= size || int64(offset+valSz) > size ||
		// Ensure that the read is within the file's size. It might be possible that
		// the offset+valSz length is beyond the file's actual size. This could happen when
		// dropAll and iterations are running simultaneously.
		int64(offset+valSz) > int64(lfSz) {
		err = io.EOF
	} else {
		buf, err = lf.f.Bytes(int(offset), int(valSz))
	}
	return buf, err
}

func (lf *LogFile) DoneWriting(offset uint32) error {
	// Sync before acquiring lock. (We call this from write() and thus know we have shared access
	// to the fd.)
	if err := lf.f.Sync(); err != nil {
		return errors.Wrapf(err, "Unable to sync value log: %q", lf.FileName())
	}

	// 写嘛 总是要锁一下的
	lf.Lock.Lock()
	defer lf.Lock.Unlock()

	// TODO: Confirm if we need to run a file sync after truncation.
	// Truncation must run after unmapping, otherwise Windows would crap itself.
	if err := lf.f.Truncature(int64(offset)); err != nil {
		return errors.Wrapf(err, "Unable to truncate file: %q", lf.FileName())
	}

	// Reinitialize the log file. This will mmap the entire file.
	if err := lf.Init(); err != nil {
		return errors.Wrapf(err, "failed to initialize file %s", lf.FileName())
	}

	// Previously we used to close the file after it was written and reopen it in read-only mode.
	// We no longer open files in read-only mode. We keep all vlog files open in read-write mode.
	return nil
}
func (lf *LogFile) Write(offset uint32, buf []byte) (err error) {
	return lf.f.AppendBuffer(offset, buf)
}
func (lf *LogFile) Truncate(offset int64) error {
	return lf.f.Truncature(offset)
}
func (lf *LogFile) Close() error {
	return lf.f.Close()
}

func (lf *LogFile) Size() int64 {
	return int64(atomic.LoadUint32(&lf.size))
}
func (lf *LogFile) AddSize(offset uint32) {
	atomic.StoreUint32(&lf.size, offset)
}

// 完成log文件的初始化
func (lf *LogFile) Bootstrap() error {
	// TODO 是否需要初始化一些内容给vlog文件?
	return nil
}

func (lf *LogFile) Init() error {
	fstat, err := lf.f.Fd.Stat()
	if err != nil {
		return errors.Wrapf(err, "Unable to check stat for %q", lf.FileName())
	}
	sz := fstat.Size()
	if sz == 0 {
		// File is empty. We don't need to mmap it. Return.
		return nil
	}
	utils.CondPanic(sz > math.MaxUint32, fmt.Errorf("[LogFile.Init] sz > math.MaxUint32"))
	lf.size = uint32(sz)
	return nil
}
func (lf *LogFile) FileName() string {
	return lf.f.Fd.Name()
}

func (lf *LogFile) Seek(offset int64, whence int) (ret int64, err error) {
	return lf.f.Fd.Seek(offset, whence)
}

func (lf *LogFile) FD() *os.File {
	return lf.f.Fd
}

// You must hold lf.lock to sync()
func (lf *LogFile) Sync() error {
	return lf.f.Sync()
}

// encodeEntry will encode entry to the buf
// layout of entry
// +--------+-----+-------+-------+
// | header | key | value | crc32 |
// +--------+-----+-------+-------+
func (lf *LogFile) EncodeEntry(e *utils.Entry, buf *bytes.Buffer, offset uint32) (int, error) {
	h := utils.Header{
		KLen:      uint32(len(e.Key)),
		VLen:      uint32(len(e.Value)),
		ExpiresAt: e.ExpiresAt,
		Meta:      e.Meta,
	}

	hash := crc32.New(utils.CastagnoliCrcTable)
	writer := io.MultiWriter(buf, hash)

	// encode header.
	var headerEnc [utils.MaxHeaderSize]byte
	sz := h.Encode(headerEnc[:])
	utils.Panic2(writer.Write(headerEnc[:sz]))
	// Encryption is disabled so writing directly to the buffer.
	utils.Panic2(writer.Write(e.Key))
	utils.Panic2(writer.Write(e.Value))
	// write crc32 hash.
	var crcBuf [crc32.Size]byte
	binary.BigEndian.PutUint32(crcBuf[:], hash.Sum32())
	utils.Panic2(buf.Write(crcBuf[:]))
	// return encoded length.
	return len(headerEnc[:sz]) + len(e.Key) + len(e.Value) + len(crcBuf), nil
}
func (lf *LogFile) DecodeEntry(buf []byte, offset uint32) (*utils.Entry, error) {
	var h utils.Header
	hlen := h.Decode(buf)
	kv := buf[hlen:]
	e := &utils.Entry{
		Meta:      h.Meta,
		ExpiresAt: h.ExpiresAt,
		Offset:    offset,
		Key:       kv[:h.KLen],
		Value:     kv[h.KLen : h.KLen+h.VLen],
	}
	return e, nil
}
