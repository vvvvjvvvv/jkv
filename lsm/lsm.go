package lsm

import (
	"github.com/vvvvjvvvv/jkv/utils"
)

type LSM struct {
	memTable   *memTable
	immutables []*memTable
	levels     *levelManager
	option     *Options
	closer     *utils.Closer
	maxMemFID  uint32
}

type Options struct {
	WorkDir            string
	MemTableSize       int64
	SSTableMaxSz       int64
	BlockSize          int     // BlockSize is the size of each block inside SSTable in bytes.
	BloomFalsePositive float64 // false positive probability of bloom filter

	// compact
	NumCompactors       int
	BaseLevelSize       int64
	LevelSizeMultiplier int // 决定 level 之间期望的 size 比例
	TableSizeMultiplier int
	BaseTableSize       int64
	NumLevelZeroTables  int
	MaxLevelNum         int

	DiscardStatsCh *chan map[uint32]int64
}

// Close _
func (lsm *LSM) Close() error {
	// 等待全部合并过程结束
	// 等待全部api调用过程结束
	lsm.closer.Close()
	// TODO 需要加锁宝成并发安全
	if lsm.memTable != nil {
		if err := lsm.memTable.close(); err != nil {
			return err
		}
	}
	for i := range lsm.immutables {
		if err := lsm.immutables[i].close(); err != nil {
			return err
		}
	}
	if err := lsm.levels.close(); err != nil {
		return err
	}
	return nil
}

// NewLSM _
func NewLSM(opt *Options) *LSM {
	lsm := &LSM{option: opt}
	// 初始化levelManager
	lsm.levels = lsm.initLevelManager(opt)
	// 启动DB恢复过程加载val，如果没有回复哪痛则创建新的内存表
	lsm.memTable, lsm.immutables = lsm.recovery()
	// 初始化closer 用于资源回收的信号控制
	lsm.closer = utils.NewCloser()
	return lsm
}

// StartCompacter _
func (lsm *LSM) StartCompacter() {
	n := lsm.option.NumCompactors
	lsm.closer.Add(n)
	for i := 0; i < n; i++ {
		go lsm.levels.runCompacter(i)
	}
}

// Set _
func (lsm *LSM) Set(entry *utils.Entry) (err error) {
	if entry == nil || len(entry.Key) == 0 {
		return utils.ErrEmptyKey
	}

	// graceful shutdown
	lsm.closer.Add(1)
	defer lsm.closer.Done()

	// 检查当前memtable是否写满，是的话：创建新的memtable，并将当前内容表写到immutables中
	// 否则写到当前memtable中
	if int64(lsm.memTable.wal.Size())+int64(utils.EstimateWalCodecSize(entry)) >
		lsm.option.MemTableSize {
		lsm.Rotato()
	}

	if err = lsm.memTable.set(entry); err != nil {
		return err
	}

	// 检查是否存在immutable需要刷盘.
	for _, immutable := range lsm.immutables {
		if err = lsm.levels.flush(immutable); err != nil {
			return err
		}
		// TODO 这里问题很大，应该用引用计数的方式回收
		err = immutable.close()
		utils.Panic(err)
	}
	if len(lsm.immutables) != 0 {
		// TODO 将lsm的immutable队列置空，这里可以优化一下节省内存空间，还可以限制一下immutable的大小为固定值
		lsm.immutables = make([]*memTable, 0)
	}
	return err
}

// Get _
func (lsm *LSM) Get(key []byte) (*utils.Entry, error) {
	if len(key) == 0 {
		return nil, utils.ErrEmptyKey
	}
	lsm.closer.Add(1)
	defer lsm.closer.Done()

	var (
		entry *utils.Entry
		err   error
	)
	// 从内容表中查询，先查活跃表，再查不变表
	if entry, err = lsm.memTable.Get(key); entry != nil && entry.Value != nil {
		return entry, err
	}
	// 从最新的immutables中开始查，版本问题
	for i := len(lsm.immutables) - 1; i >= 0; i-- {
		if entry, err = lsm.immutables[i].Get(key); entry != nil && entry.Value != nil {
			return entry, err
		}
	}
	// 从level manager查询
	return lsm.levels.Get(key)
}

func (lsm *LSM) MemSize() int64 {
	return lsm.memTable.Size()
}

func (lsm *LSM) MemTableIsNil() bool {
	return lsm.memTable == nil
}

func (lsm *LSM) GetSkipListFromMemTable() *utils.Skiplist {
	return lsm.memTable.sl
}

func (lsm *LSM) Rotato() {
	lsm.immutables = append(lsm.immutables, lsm.memTable)
	lsm.memTable = lsm.NewMemTable()
}
