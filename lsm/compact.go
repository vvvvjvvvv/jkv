package lsm

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vvvvjvvvv/jkv/pb"
	"github.com/vvvvjvvvv/jkv/utils"
)

// 归并优先级
type compactionPriority struct {
	level        int
	score        float64
	adjusted     float64
	dropPrefixes [][]byte
	t            targets
}

// 归并目标
type targets struct {
	baseLevel int
	targetSz  []int64
	fileSz    []int64
}

// 压缩计划
type compactDef struct {
	compactorId int
	t           targets
	p           compactionPriority
	thisLevel   *levelHandler
	nextLevel   *levelHandler

	top []*table
	bot []*table

	thisRange keyRange
	nextRange keyRange
	splits    []keyRange

	thisSize int64

	dropPrefixes [][]byte
}

func (cd *compactDef) lockLevels() {
	cd.thisLevel.RLock()
	cd.nextLevel.RLock()
}

func (cd *compactDef) unlockLevels() {
	cd.thisLevel.RUnlock()
	cd.nextLevel.RUnlock()
}

// runCompacter 启动一个 compacter
func (lm *levelManager) runCompacter(id int) {
	defer lm.lsm.closer.Done()

	randomDelay := time.NewTimer(time.Duration(rand.Int31n(1000)) * time.Millisecond)
	select {
	case <-randomDelay.C:
	case <-lm.lsm.closer.CloseSignal:
		randomDelay.Stop()
		return
	}
	// TODO 这个值有待验证
	ticker := time.NewTicker(50000 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		// can add a done channel or other stuff.
		case <-ticker.C:
			lm.runOnce(id)
		case <-lm.lsm.closer.CloseSignal:
			return
		}
	}
}

// runOnce
func (lm *levelManager) runOnce(id int) bool {
	prios := lm.pickCompactLevels()
	if id == 0 {
		// 0号协程 总是倾向于压缩0层
		prios = moveL0Front(prios)
	}
	for _, p := range prios {
		if id == 0 && p.level == 0 {
			// 对于l0 无论多少得分都要运行
		} else if p.adjusted < 1.0 {
			// 对于其他level 得分小于 就不执行
			break
		}
		if lm.run(id, p) {
			return true
		}
	}
	return false
}

func moveL0Front(prios []compactionPriority) []compactionPriority {
	idx := -1
	for i, p := range prios {
		if p.level == 0 {
			idx = i
			break
		}
	}

	// If idx == =1, we didn't find L0
	// If idx == 0,  we don't need to do anything. L0 is already at the front.
	if idx > 0 {
		out := append([]compactionPriority{}, prios[idx])
		out = append(out, prios[:idx]...)
		out = append(out, prios[idx+1:]...)
		return out
	}
	return prios
}

// run 执行一个优先级指定的合并任务
func (lm *levelManager) run(id int, p compactionPriority) bool {
	err := lm.doCompact(id, p)
	switch err {
	case nil:
		return true
	case utils.ErrFillTables:
		// 什么都不做，此时合并过程被忽略
	default:
		log.Printf("[taskId:%d] While running doCompac6tt: %v\n", id, err)
	}
	return false
}

// doCompact 选择level的某些表合并到目标level
func (lm *levelManager) doCompact(id int, p compactionPriority) error {
	l := p.level
	utils.CondPanic(l >= lm.opt.MaxLevelNum, errors.New("[doCompact] Sanity check. l >= lm.opt.MaxLevelNum")) // sanity check

	if p.t.baseLevel == 0 {
		p.t = lm.levelTargets()
	}

	//创建真正的压缩计划
	cd := compactDef{
		compactorId:  id,
		p:            p,
		t:            p.t,
		thisLevel:    lm.levels[l],
		dropPrefixes: p.dropPrefixes,
	}

	// 如果是第0层，对齐单独填充处理
	if l == 0 {
		cd.nextLevel = lm.levels[p.t.baseLevel]
		if !lm.fillTablesL0(&cd) {
			return utils.ErrFillTables
		}
	} else {
		cd.nextLevel = cd.thisLevel
		// 如果不是最后一层，则压缩到下一层即可
		if !cd.thisLevel.isLastLevel() {
			cd.nextLevel = lm.levels[l+1]
		}
		if !lm.fillTables(&cd) {
			return utils.ErrFillTables
		}
	}
	// 完成合并后，从合并状态中删除
	defer lm.compactState.delete(cd) // remove the ranges from compaction status.

	// 执行合并计划
	if err := lm.runCompactDef(id, l, cd); err != nil {
		// This compaction couldn't be done successfully.
		log.Printf("[Compactor: %d] LOG Compact FAILED with error: %+v: %+v", id, err, cd)
		return err
	}

	log.Printf("[Compactor: %d] Compaction for level: %d DONE", id, cd.thisLevel.levelNum)

	return nil
}

// pickCompactLevel 选择合适的level执行合并，返回判断的优先级
func (lm *levelManager) pickCompactLevels() (prios []compactionPriority) {
	t := lm.levelTargets()
	addPriority := func(level int, score float64) {
		pri := compactionPriority{
			level:    level,
			score:    score,
			adjusted: score,
			t:        t,
		}
		prios = append(prios, pri)
	}

	// 根据l0表的table数量来对压缩提权
	addPriority(0, float64(lm.levels[0].numTables())/float64(lm.opt.NumLevelZeroTables))

	// 非l0层都根据大小计算优先级
	for i := 1; i < len(lm.levels); i++ {
		// 处于压缩状态的sst不能计算在内
		delSize := lm.compactState.delSize(i)
		l := lm.levels[i]
		sz := l.getTotalSize() - delSize
		// score的计算是 扣除正在合并的表后的尺寸与目标sz的比值
		addPriority(i, float64(sz)/float64(t.targetSz[i]))
	}
	// l0层是数量，非l0层是size

	utils.CondPanic(len(prios) != len(lm.levels), errors.New("[pickCompactLegels] len(prios) != len(lm.levels)"))

	// 调整得分
	var prevLevel int
	for level := t.baseLevel; level < len(lm.levels); level++ {
		if prios[prevLevel].adjusted >= 1 {
			// 避免过大的得分
			const minScore = 0.01
			if prios[level].score >= minScore {
				prios[prevLevel].adjusted /= prios[level].adjusted
			} else {
				prios[prevLevel].adjusted /= minScore
			}
		}
		prevLevel = level
	}

	// 仅选择得分大于1的压缩内容，并且允许l0到l0的特殊压缩，为了提升查询性能允许l0层独自压缩
	out := prios[:0]
	for _, p := range prios[:len(prios)-1] {
		if p.score >= 1.0 {
			out = append(out, p)
		}
	}
	prios = out

	// 按照优先级排序
	sort.Slice(prios, func(i, j int) bool {
		return prios[i].adjusted > prios[j].adjusted
	})

	return prios
}

func (lm *levelManager) lastLevel() *levelHandler {
	return lm.levels[len(lm.levels)-1]
}

// levelTargets
func (lm *levelManager) levelTargets() targets {
	adjust := func(sz int64) int64 {
		if sz < lm.opt.BaseLevelSize {
			return lm.opt.BaseLevelSize
		}
		return sz
	}

	// 初始化默认值都是最大层级
	t := targets{
		targetSz: make([]int64, len(lm.levels)),
		fileSz:   make([]int64, len(lm.levels)),
	}

	// 从最后一个level开始计算
	dbSize := lm.lastLevel().getTotalSize()
	for i := len(lm.levels) - 1; i > 0; i-- {
		levelTargetSize := adjust(dbSize)
		t.targetSz[i] = levelTargetSize
		// 如果当前的level没有达到合并的要求
		if t.baseLevel == 0 && levelTargetSize <= lm.opt.BaseLevelSize {
			t.baseLevel = i
		}
		dbSize /= int64(lm.opt.LevelSizeMultiplier)
	}

	tsz := lm.opt.BaseTableSize
	for i := 0; i < len(lm.levels); i++ {
		if i == 0 {
			// l0 选择memtable的size作为文件的尺寸
			t.fileSz[i] = lm.opt.MemTableSize
		} else if i <= t.baseLevel {
			t.fileSz[i] = tsz
		} else {
			tsz *= int64(lm.opt.TableSizeMultiplier)
			t.fileSz[i] = tsz
		}
	}

	// 找到最后一个空level作为目标level实现跨level归并，减少写放大
	for i := t.baseLevel + 1; i < len(lm.levels); i++ {
		if lm.levels[i].getTotalSize() > 0 {
			break
		}
		t.baseLevel = i
	}

	// 如果存在断层，则目标level++
	b := t.baseLevel
	lvl := lm.levels
	if b < len(lvl)-1 && lvl[b].getTotalSize() == 0 && lvl[b+1].getTotalSize() < t.targetSz[b+1] {
		t.baseLevel++
	}
	return t
}

type thisAndNextLevelRLocked struct {
}

func (lm *levelManager) fillTables(cd *compactDef) bool {
	cd.lockLevels()
	defer cd.unlockLevels()

	tables := make([]*table, cd.thisLevel.numTables())
	copy(tables, cd.thisLevel.tables)
	if len(tables) == 0 {
		return false
	}
	// We're doing a maxLevel to maxLevel compaction. Pick tables based on the stale data size.
	if cd.thisLevel.isLastLevel() {
		return lm.fillMaxLevelTables(tables, cd)
	}
	// We pick tables, so we compact older tables first. This is similar to
	// kOldestLargestSeqFirst in RocksDB.
	lm.sortByHeuristic(tables, cd)

	for _, t := range tables {
		cd.thisSize = t.Size()
		cd.thisRange = getKeyRange(t)
		// 如果被压缩过了，则什么都不需要做
		if lm.compactState.overlapsWith(cd.thisLevel.levelNum, cd.thisRange) {
			continue
		}
		cd.top = []*table{t}
		left, right := cd.nextLevel.overlappingTables(levelHandlerRLocked{}, cd.thisRange)

		cd.bot = make([]*table, right-left)
		copy(cd.bot, cd.nextLevel.tables[left:right])

		if len(cd.bot) == 0 {
			cd.bot = []*table{}
			cd.nextRange = cd.thisRange
			if !lm.compactState.compareAndAdd(thisAndNextLevelRLocked{}, *cd) {
				continue
			}
			return true
		}
		cd.nextRange = getKeyRange(cd.bot...)

		if lm.compactState.overlapsWith(cd.nextLevel.levelNum, cd.nextRange) {
			continue
		}
		if !lm.compactState.compareAndAdd(thisAndNextLevelRLocked{}, *cd) {
			continue
		}
		return true
	}
	return false
}

// compact oldest tables first.
func (lm *levelManager) sortByHeuristic(tables []*table, cd *compactDef) {
	if len(tables) == 0 || cd.nextLevel == nil {
		return
	}

	// sort tables by max version. This is what RocksDB does.
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].ss.Indexs().MaxVersion < tables[j].ss.Indexs().MaxVersion
	})
}

func (lm *levelManager) runCompactDef(id, l int, cd compactDef) (err error) {
	if len(cd.t.fileSz) == 0 {
		return errors.New("Filesizes cannot be zero. Targets are not set")
	}
	timeStart := time.Now()

	thisLevel := cd.thisLevel
	nextLevel := cd.nextLevel

	utils.CondPanic(len(cd.splits) != 0, errors.New("len(cd.splits) != 0"))
	if thisLevel == nextLevel {
		// l0 to l0 和 lmax to lmax 不做特殊处理
	} else {
		lm.addSplits(&cd)
	}
	// 追加一个空的
	if len(cd.splits) == 0 {
		cd.splits = append(cd.splits, keyRange{})
	}

	newTables, decr, err := lm.compactBuildTables(l, cd)
	if err != nil {
		return err
	}
	defer func() {
		// Only assign to err, if it's not already nil.
		if decErr := decr(); err == nil {
			err = decErr
		}
	}()
	changeSet := buildChangeSet(&cd, newTables)
	// 删除之前先更新manifest文件
	if err := lm.manifestFile.AddChanges(changeSet.Changes); err != nil {
		return err
	}

	if err := nextLevel.replaceTables(cd.bot, newTables); err != nil {
		return err
	}
	defer decrRefs(cd.top)
	if err := thisLevel.deleteTables(cd.top); err != nil {
		return err
	}

	from := append(tablesToString(cd.top), tablesToString(cd.bot)...)
	to := tablesToString(newTables)
	if dur := time.Since(timeStart); dur > 2*time.Second {
		var expensive string
		if dur > time.Second {
			expensive = " [E]"
		}
		fmt.Printf("[%d]%s LOG Compact %d->%d (%d, %d -> %d tables with %d splits)."+
			" [%s] -> [%s], took %v\n",
			id, expensive, thisLevel.levelNum, nextLevel.levelNum, len(cd.top), len(cd.bot),
			len(newTables), len(cd.splits), strings.Join(from, " "), strings.Join(to, " "),
			dur.Round(time.Millisecond))
	}
	return nil
}

// tablesToString
func tablesToString(tables []*table) []string {
	var res []string
	for _, t := range tables {
		res = append(res, fmt.Sprintf("%05d", t.fid))
	}
	res = append(res, ".")
	return res
}

// buildChangeSet _
func buildChangeSet(cd *compactDef, newTables []*table) pb.ManifestChangeSet {
	changes := []*pb.ManifestChange{}
	for _, table := range newTables {
		changes = append(changes, newCreateChange(table.fid, cd.nextLevel.levelNum))
	}
	for _, table := range cd.top {
		changes = append(changes, newDeleteChange(table.fid))
	}
	for _, table := range cd.bot {
		changes = append(changes, newDeleteChange(table.fid))
	}
	return pb.ManifestChangeSet{Changes: changes}
}

func newDeleteChange(id uint64) *pb.ManifestChange {
	return &pb.ManifestChange{
		Id: id,
		Op: pb.ManifestChange_DELETE,
	}
}

// newCreateChange
func newCreateChange(id uint64, level int) *pb.ManifestChange {
	return &pb.ManifestChange{
		Id:    id,
		Op:    pb.ManifestChange_CREATE,
		Level: uint32(level),
	}
}

// compactBuildTables 合并两个层的sst文件
func (lm *levelManager) compactBuildTables(lev int, cd compactDef) ([]*table, func() error, error) {
	topTables := cd.top
	botTables := cd.bot
	iterOpt := &utils.Options{
		IsAsc: true,
	}
	//numTables := int64(len(topTables) + len(botTables))
	newIterator := func() []utils.Iterator {
		// Create iterators across all the tables involved first.
		var iters []utils.Iterator
		switch {
		case lev == 0:
			iters = append(iters, iteratorsReversed(topTables, iterOpt)...)
		case len(topTables) > 0:
			iters = []utils.Iterator{topTables[0].NewIterator(iterOpt)}
		}
		return append(iters, NewConcatIterator(botTables, iterOpt))
	}

	// 开始并行执行压缩过程
	res := make(chan *table, 3)
	inflightBuilders := utils.NewThrottle(8 + len(cd.splits))
	for _, kr := range cd.splits {
		// Initiate Do here so we can register the goroutines for buildTables too.
		if err := inflightBuilders.Do(); err != nil {
			return nil, nil, fmt.Errorf("cannot start subcompaction: %+v", err)
		}
		// 开启一个协程去处理子压缩
		go func(kr keyRange) {
			defer inflightBuilders.Done(nil)
			it := NewMergeIterator(newIterator(), false)
			defer it.Close()
			lm.subcompact(it, kr, cd, inflightBuilders, res)
		}(kr)
	}

	// mapreduce的方式收集table的句柄
	var newTables []*table
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for t := range res {
			newTables = append(newTables, t)
		}
	}()

	// 在这里等待所有的压缩过程完成
	err := inflightBuilders.Finish()
	// channel 资源回收
	close(res)
	// 等待所有的builder刷到磁盘
	wg.Wait()

	if err == nil {
		// 同步刷盘，保证数据一定落盘
		err = utils.SyncDir(lm.opt.WorkDir)
	}

	if err != nil {
		// 如果出现错误，则删除索引新创建的文件
		_ = decrRefs(newTables)
		return nil, nil, fmt.Errorf("while running compactions for: %+v, %v", cd, err)
	}

	sort.Slice(newTables, func(i, j int) bool {
		return utils.CompareKeys(newTables[i].ss.MaxKey(), newTables[j].ss.MaxKey()) < 0
	})
	return newTables, func() error { return decrRefs(newTables) }, nil
}

// 并行的运行子压缩情况
func (lm *levelManager) addSplits(cd *compactDef) {
	cd.splits = cd.splits[:0]

	// Let's say we have 10 tables in cd.bot and min width = 3. Then, we'll pick
	// 0, 1, 2 (pick), 3, 4, 5 (pick), 6, 7, 8 (pick), 9 (pick, because last table).
	// This gives us 4 picks for 10 tables.
	// In an edge case, 142 tables in bottom led to 48 splits. That's too many splits, because it
	// then uses up a lot of memory for table builder.
	// We should keep it so we have at max 5 splits.
	width := int(math.Ceil(float64(len(cd.bot)) / 5.0))
	if width < 3 {
		width = 3
	}
	skr := cd.thisRange
	skr.extend(cd.nextRange)

	addRange := func(right []byte) {
		skr.right = utils.Copy(right)
		cd.splits = append(cd.splits, skr)
		skr.left = skr.right
	}

	for i, t := range cd.bot {
		// last entry in bottom table.
		if i == len(cd.bot)-1 {
			addRange([]byte{})
			return
		}
		if i%width == width-1 {
			// 设置最大值为右区间
			right := utils.KeyWithTs(utils.ParseKey(t.ss.MaxKey()), math.MaxUint64)
			addRange(right)
		}
	}
}

// sortByStaleData 对表中陈旧数据的数量对sst文件进行排序
func (lm *levelManager) sortByStaleDataSize(tables []*table, cd *compactDef) {
	if len(tables) == 0 || cd.nextLevel == nil {
		return
	}
	// TODO 统计一个sst文件中成就数据的数量，设计对存储格式的修改
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].StaleDataSize() > tables[j].StaleDataSize()
	})
}

// compaction in max level
func (lm *levelManager) fillMaxLevelTables(tables []*table, cd *compactDef) bool {
	sortedTables := make([]*table, len(tables))
	copy(sortedTables, tables)
	lm.sortByStaleDataSize(sortedTables, cd)

	if len(sortedTables) > 0 && sortedTables[0].StaleDataSize() == 0 {
		// This is a maxLevel to maxLevel compaction and we don't have any stale data.
		return false
	}
	cd.bot = []*table{}
	collectBotTables := func(t *table, needSz int64) {
		totalSize := t.Size()

		j := sort.Search(len(tables), func(i int) bool {
			return utils.CompareKeys(tables[i].ss.MinKey(), t.ss.MinKey()) >= 0
		})
		utils.CondPanic(tables[j].fid != t.fid, errors.New("tables[j].ID() != t.ID()"))
		j++
		// Collect tables until we reach the the required size.
		for j < len(tables) {
			newT := tables[j]
			totalSize += newT.Size()

			if totalSize >= needSz {
				break
			}
			cd.bot = append(cd.bot, newT)
			cd.nextRange.extend(getKeyRange(newT))
			j++
		}
	}
	now := time.Now()
	for _, t := range sortedTables {
		if now.Sub(*t.GetCreatedAt()) < time.Hour {
			// Just created it an hour ago. Don't pick for compaction.
			continue
		}
		// If the stale data size is less than 10 MB, it might not be worth
		// rewriting the table. Skip it.
		if t.StaleDataSize() < 10<<20 {
			continue
		}

		cd.thisSize = t.Size()
		cd.thisRange = getKeyRange(t)
		// Set the next range as the same as the current range. If we don't do
		// this, we won't be able to run more than one max level compactions.
		cd.nextRange = cd.thisRange
		// If we're already compacting this range, don't do anything.
		if lm.compactState.overlapsWith(cd.thisLevel.levelNum, cd.thisRange) {
			continue
		}

		// Found a valid table!
		cd.top = []*table{t}

		needFileSz := cd.t.fileSz[cd.thisLevel.levelNum]
		// 如果合并的sst size需要的文件尺寸直接终止
		if t.Size() >= needFileSz {
			break
		}
		// TableSize is less than what we want. Collect more tables for compaction.
		// If the level has multiple small tables, we collect all of them
		// together to form a bigger table.
		collectBotTables(t, needFileSz)
		if !lm.compactState.compareAndAdd(thisAndNextLevelRLocked{}, *cd) {
			cd.bot = cd.bot[:0]
			cd.nextRange = keyRange{}
			continue
		}
		return true
	}
	if len(cd.top) == 0 {
		return false
	}

	return lm.compactState.compareAndAdd(thisAndNextLevelRLocked{}, *cd)
}

// fillTablesL0 先尝试从l0到lbase的压缩，如果失败则对l0自己压缩
func (lm *levelManager) fillTablesL0(cd *compactDef) bool {
	if ok := lm.fillTablesL0ToLbase(cd); ok {
		return true
	}
	return lm.fillTablesL0ToL0(cd)
}

func (lm *levelManager) fillTablesL0ToLbase(cd *compactDef) bool {
	if cd.nextLevel.levelNum == 0 {
		utils.Panic(errors.New("base level cannot be zero"))
	}

	// 如果优先级低于1 则不执行
	if cd.p.adjusted > 0.0 && cd.p.adjusted < 1.0 {
		// Do not compact to Lbase if adjusted score is less than 1.0
		return false
	}
	cd.lockLevels()
	defer cd.unlockLevels()

	top := cd.thisLevel.tables
	if len(top) == 0 {
		return false
	}

	var out []*table
	var kr keyRange
	// cd.top[0] 是最老的文件，从最老的文件开始
	for _, t := range top {
		dkr := getKeyRange(t)
		if kr.overlapsWith(dkr) {
			out = append(out, t)
			kr.extend(dkr)
		} else {
			// 如果有任何一个不重合的区间存在 则直接终止
			break
		}
	}

	// 获取源(l0)range list 的全局 range 对象
	cd.thisRange = getKeyRange(out...)
	cd.top = out

	left, right := cd.nextLevel.overlappingTables(levelHandlerRLocked{}, cd.thisRange)
	cd.bot = make([]*table, right-left)
	copy(cd.bot, cd.nextLevel.tables[left:right])

	if len(cd.bot) == 0 {
		cd.nextRange = cd.thisRange
	} else {
		cd.nextRange = getKeyRange(cd.bot...)
	}

	return lm.compactState.compareAndAdd(thisAndNextLevelRLocked{}, *cd)
}

func (lm *levelManager) fillTablesL0ToL0(cd *compactDef) bool {
	if cd.compactorId != 0 {
		// 只要0号压缩处理器可以执行，避免l0tol0的资源竞争
		return false
	}

	cd.nextLevel = lm.levels[0]
	cd.nextRange = keyRange{}
	cd.bot = nil

	// TODO 这里是否会导致死锁？
	utils.CondPanic(cd.thisLevel.levelNum != 0, errors.New("cd.thisLevel.levelNum != 0"))
	utils.CondPanic(cd.nextLevel.levelNum != 0, errors.New("cd.nextLevel.levelNum != 0"))
	lm.levels[0].RLock()
	defer lm.levels[0].RUnlock()

	lm.compactState.Lock()
	defer lm.compactState.Unlock()

	top := cd.thisLevel.tables
	var out []*table
	now := time.Now()
	for _, t := range top {
		if t.Size() >= 2*cd.t.fileSz[0] {
			// 在L0 to L0 的压缩过程中，不要对过大的sst文件压缩，这会造成性能抖动
			continue
		}
		if now.Sub(*t.GetCreatedAt()) < 10*time.Second {
			// 如果sst的创建时间不足10s 也不要回收
			continue
		}
		// 如果当前的sst 已经在压缩状态 也应该忽略
		if _, beingCompacted := lm.compactState.tables[t.fid]; beingCompacted {
			continue
		}
		out = append(out, t)
	}

	if len(out) < 4 {
		// 满足条件的sst小于4个那就不压缩了
		return false
	}
	cd.thisRange = infRange
	cd.top = out

	// 在这个过程中避免任何l0到其他层的合并
	thisLevel := lm.compactState.levels[cd.thisLevel.levelNum]
	thisLevel.ranges = append(thisLevel.ranges, infRange)
	for _, t := range out {
		lm.compactState.tables[t.fid] = struct{}{}
	}

	//  l0 to l0的压缩最终都会压缩为一个文件，这大大减少了l0层文件数量，减少了读放大
	cd.t.fileSz[0] = math.MaxUint32
	return true
}

// getKeyRange 返回一组sst的区间合并后的最大值与最小值
func getKeyRange(tables ...*table) keyRange {
	if len(tables) == 0 {
		return keyRange{}
	}

	minKey := tables[0].ss.MinKey()
	maxKey := tables[0].ss.MaxKey()
	for i := 1; i < len(tables); i++ {
		if utils.CompareKeys(tables[i].ss.MinKey(), minKey) < 0 {
			minKey = tables[i].ss.MinKey()
		}
		if utils.CompareKeys(tables[i].ss.MaxKey(), maxKey) > 0 {
			maxKey = tables[i].ss.MaxKey()
		}
	}

	// We pick all the versions of the smallest and the biggest key, Note that version zero would
	// be the rightmost key, considering version are default sorted in descending order.
	return keyRange{
		left:  utils.KeyWithTs(utils.ParseKey(minKey), math.MaxUint64),
		right: utils.KeyWithTs(utils.ParseKey(maxKey), 0),
	}
}

// l0 层是通过fid排序的
func iteratorsReversed(th []*table, opt *utils.Options) []utils.Iterator {
	out := make([]utils.Iterator, 0, len(th))
	for i := len(th) - 1; i >= 0; i-- {
		// This will increment the reference of the table handler.
		out = append(out, th[i].NewIterator(opt))
	}
	return out
}

func (lm *levelManager) updateDiscardStats(discardStats map[uint32]int64) {
	select {
	case *lm.lsm.option.DiscardStatsCh <- discardStats:
	default:
	}
}

// 真正执行并行压缩的子压缩文件
func (lm *levelManager) subcompact(it utils.Iterator, kr keyRange, cd compactDef,
	inflightBuilders *utils.Throttle, res chan<- *table) {
	var lastKey []byte
	// 更新 discardStats
	discardStats := make(map[uint32]int64)
	defer func() {
		lm.updateDiscardStats(discardStats)
	}()
	updateStats := func(e *utils.Entry) {
		if e.Meta&utils.BitValuePointer > 0 {
			var vp utils.ValuePtr
			vp.Decode(e.Value)
			discardStats[vp.Fid] += int64(vp.Len)
		}
	}
	addKeys := func(builder *tableBuilder) {
		var tableKr keyRange
		for ; it.Valid(); it.Next() {
			key := it.Item().Entry().Key
			//version := utils.ParseTs(key)
			isExpired := IsDeletedOrExpired(it.Item().Entry())
			if !utils.SameKey(key, lastKey) {
				// 如果迭代器返回的key大于当前key的范围就不用执行了
				if len(kr.right) > 0 && utils.CompareKeys(key, kr.right) >= 0 {
					break
				}
				if builder.ReachedCapacity() {
					// 如果超过预估的sst文件大小，则直接结束
					break
				}
				// 把当前的key变为 lastKey
				lastKey = utils.SafeCopy(lastKey, key)
				//umVersions = 0
				// 如果左边界没有，则当前key给到左边界
				if len(tableKr.left) == 0 {
					tableKr.left = utils.SafeCopy(tableKr.left, key)
				}
				// 更新右边界
				tableKr.right = lastKey
			}
			// TODO 这里要区分值的指针
			// 判断是否是过期内容，是的话就删除
			switch {
			case isExpired:
				updateStats(it.Item().Entry())
				builder.AddStaleKey(it.Item().Entry())
			default:
				builder.AddKey(it.Item().Entry())
			}
		}
	} // End of function: addKeys

	//如果 key range left还存在 则seek到这里 说明遍历中途停止了
	if len(kr.left) > 0 {
		it.Seek(kr.left)
	} else {
		// 迭代器的初始化，移到最前面的部分
		it.Rewind()
	}
	for it.Valid() {
		key := it.Item().Entry().Key
		if len(kr.right) > 0 && utils.CompareKeys(key, kr.right) >= 0 {
			break
		}
		// 拼装table创建的参数
		// TODO 这里可能要大改，对open table的参数复制一份opt
		builder := newTableBuilerWithSSTSize(lm.opt, cd.t.fileSz[cd.nextLevel.levelNum])

		// This would do the iteration and add keys to builder.
		addKeys(builder)

		// It was true that it.Valid() at least once in the loop above, which means we
		// called Add() at least once, and builder is not Empty().
		if builder.empty() {
			// Cleanup builder resources:
			builder.finish()
			builder.Close()
			continue
		}
		if err := inflightBuilders.Do(); err != nil {
			// Can't return from here, until I decrRef all the tables that I built so far.
			break
		}
		// 充分发挥 ssd的并行 写入特性
		go func(builder *tableBuilder) {
			defer inflightBuilders.Done(nil)
			defer builder.Close()
			var tbl *table
			newFID := atomic.AddUint64(&lm.maxFID, 1) // compact的时候是没有memtable的，这里自增maxFID即可。
			// TODO 这里的sst文件需要根据level大小变化
			sstName := utils.FileNameSSTable(lm.opt.WorkDir, newFID)
			tbl = openTable(lm, sstName, builder)
			if tbl == nil {
				return
			}
			res <- tbl
		}(builder)
	}
}

// 判断是否过期 是可删除
func IsDeletedOrExpired(e *utils.Entry) bool {
	if e.Value == nil {
		return true
	}
	if e.ExpiresAt == 0 {
		return false
	}

	return e.ExpiresAt <= uint64(time.Now().Unix())
}

// compactStatus
type compactStatus struct {
	sync.RWMutex
	levels []*levelCompactStatus
	tables map[uint64]struct{}	// 记录处于压缩状态的tables
}

func (lsm *LSM) newCompactStatus() *compactStatus {
	cs := &compactStatus{
		levels: make([]*levelCompactStatus, 0),
		tables: make(map[uint64]struct{}),
	}
	for i := 0; i < lsm.option.MaxLevelNum; i++ {
		cs.levels = append(cs.levels, &levelCompactStatus{})
	}
	return cs
}

func (cs *compactStatus) overlapsWith(level int, this keyRange) bool {
	cs.RLock()
	defer cs.RUnlock()

	thisLevel := cs.levels[level]
	return thisLevel.overlapsWith(this)
}

func (cs *compactStatus) delSize(l int) int64 {
	cs.RLock()
	defer cs.RUnlock()
	return cs.levels[l].delSize
}

func (cs *compactStatus) delete(cd compactDef) {
	cs.Lock()
	defer cs.Unlock()

	tl := cd.thisLevel.levelNum

	thisLevel := cs.levels[cd.thisLevel.levelNum]
	nextLevel := cs.levels[cd.nextLevel.levelNum]

	thisLevel.delSize -= cd.thisSize
	found := thisLevel.remove(cd.thisRange)
	// The following check makes sense only if we're compacting more than one
	// table. In case of the max level, we might rewrite a single table to
	// remove stale data.
	if cd.thisLevel != cd.nextLevel && !cd.nextRange.isEmpty() {
		found = nextLevel.remove(cd.nextRange) && found
	}
	if !found {
		this := cd.thisRange
		next := cd.nextRange
		fmt.Printf("Looking for: %s in this level %d\n", this, tl)
		fmt.Printf("This level:\n%s\n", thisLevel.debug())
		fmt.Println()
		fmt.Printf("Looking for: %s in next level %d\n", next, cd.nextLevel.levelNum)
		fmt.Printf("Next level:\n%s\n", nextLevel.debug())
		log.Fatal("KeyRange not found")
	}

	for _, t := range append(cd.top, cd.bot...) {
		_, ok := cs.tables[t.fid]
		utils.CondPanic(!ok, fmt.Errorf("cs.tables is nil"))
		delete(cs.tables, t.fid)
	}
}

func (cs *compactStatus) compareAndAdd(_ thisAndNextLevelRLocked, cd compactDef) bool {
	cs.Lock()
	defer cs.Unlock()

	tl := cd.thisLevel.levelNum
	utils.CondPanic(tl >= len(cs.levels), fmt.Errorf("Got level %d. Max levels: %d", tl, len(cs.levels)))
	thisLevel := cs.levels[cd.thisLevel.levelNum]
	nextLevel := cs.levels[cd.nextLevel.levelNum]

	// 基于区间比较，而不是tables
	if thisLevel.overlapsWith(cd.thisRange) {
		return false
	}
	if nextLevel.overlapsWith(cd.nextRange) {
		return false
	}

	thisLevel.ranges = append(thisLevel.ranges, cd.thisRange)
	nextLevel.ranges = append(nextLevel.ranges, cd.nextRange)
	thisLevel.delSize += cd.thisSize
	for _, t := range append(cd.top, cd.bot...) {
		cs.tables[t.fid] = struct{}{}
	}

	return true
}

// levelCompactStatus
type levelCompactStatus struct {
	ranges  []keyRange
	delSize int64
}

func (lcs *levelCompactStatus) overlapsWith(dst keyRange) bool {
	for _, r := range lcs.ranges {
		if r.overlapsWith(dst) {
			return true
		}
	}
	return false
}

func (lcs *levelCompactStatus) remove(dst keyRange) bool {
	final := lcs.ranges[:0]
	var found bool
	for _, r := range lcs.ranges {
		if !r.equals(dst) {
			final = append(final, r)
		} else {
			found = true
		}
	}
	lcs.ranges = final
	return found
}

func (lcs *levelCompactStatus) debug() string {
	var b bytes.Buffer
	for _, r := range lcs.ranges {
		b.WriteString(r.String())
	}
	return b.String()
}

// keyRange
type keyRange struct {
	left  []byte
	right []byte
	inf   bool
	size  int64 // size is used for key splits.
}

var infRange = keyRange{
	inf: true,
}

func (r keyRange) isEmpty() bool {
	return len(r.left) == 0 && len(r.right) == 0 && !r.inf
}

func (r keyRange) equals(dst keyRange) bool {
	return bytes.Equal(r.left, dst.left) &&
		bytes.Equal(r.right, dst.right) &&
		r.inf == dst.inf
}

func (r keyRange) String() string {
	return fmt.Sprintf("[left=%x, right=%x, inf=%v]", r.left, r.right, r.inf)
}

func (r *keyRange) extend(kr keyRange) {
	if kr.isEmpty() {
		return
	}

	if r.isEmpty() {
		*r = kr
	}

	if len(r.left) == 0 || utils.CompareKeys(kr.left, r.left) < 0 {
		r.left = kr.left
	}

	if len(r.right) == 0 || utils.CompareKeys(kr.right, r.right) > 0 {
		r.right = kr.right
	}

	if kr.inf {
		r.inf = true
	}
}

func (r keyRange) overlapsWith(dst keyRange) bool {
	// Empty keyRange always overlaps/
	if r.isEmpty() {
		return true
	}

	// Empty dst don't everlap with anythings.
	if dst.isEmpty() {
		return false
	}

	// [dst.left, dst.right] ... [r.left, r.right]
	// if my left is greater than dst right, we have no overlap.
	if utils.CompareKeys(r.left, dst.right) > 0 {
		return false
	}

	// [r.left, r.right] ... [dst.left, dst.right]
	// if my right is smaller than dst.left, we have no overlap.
	if utils.CompareKeys(r.right, dst.left) < 0 {
		return false
	}

	return true
}
