package bitcask

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/zach030/tiny-bitcask/internal"
	"github.com/zach030/tiny-bitcask/utils"
)

const (
	ArchivedDataFile = "bitcask"
	DataFileExt      = ".data"
	IndexFile        = "index"
)

type BitCask struct {
	path string
	lock sync.RWMutex

	indexer *internal.KeyDir
	curr    *internal.BkFile

	dataFiles map[int]*internal.BkFile

	config *Config

	// 是否在合并
	isMerging bool
}

// Open database
func Open(path string, options ...Option) (*BitCask, error) {
	//todo 指定配置文件路径
	var cfg = DefaultConfig
	for _, option := range options {
		if err := option(cfg); err != nil {
			return nil, err
		}
	}
	err := os.MkdirAll(path, 0700)
	if err != nil {
		return nil, err
	}
	db := &BitCask{
		path:   path,
		config: cfg,
	}
	err = db.rebuild()
	if err != nil {
		return nil, err
	}
	return db, nil
}

// rebuild load from bitcask.hint file to build index
func (b *BitCask) rebuild() (err error) {
	dfs, last, err := loadDataFiles(b.path)
	if err != nil {
		return
	}
	idx, err := loadIndexes(b.path)
	if err != nil {
		return
	}
	curr, err := internal.NewBkFile(b.path, last, true)
	if err != nil {
		return
	}
	b.curr = curr
	b.indexer = idx
	b.dataFiles = dfs
	return
}

// Get Retrieve a value by key from a Bitcask datastore.
func (b *BitCask) Get(key []byte) ([]byte, error) {
	// 先从内存索引中获取此记录的信息，通过一次磁盘随机IO获取数据
	item, ok := b.indexer.Get(key)
	if !ok {
		return nil, ErrSpecifyKeyNotExist
	}
	bk := b.curr
	// 读到的item所在文件可能是active和older
	if !b.isInActiveFile(item.FileID) {
		bk = b.dataFiles[item.FileID]
	}
	e, err := bk.Read(item.ValuePos, item.ValueSize)
	if err != nil {
		return nil, err
	}
	if !e.IsValid() {
		return nil, ErrInvalidCheckSum
	}
	return e.Value(), nil
}

func (b *BitCask) Has(key []byte) bool {
	b.lock.RLock()
	defer b.lock.RUnlock()
	_, ok := b.indexer.Get(key)
	return ok
}

// Put Store a key and value in a Bitcask datastore.
func (b *BitCask) Put(key, value []byte) error {
	err := b.validKV(key, value)
	if err != nil {
		return err
	}
	b.lock.Lock()
	defer b.lock.Unlock()
	pos, size, err := b.put(key, value)
	if err != nil {
		return err
	}
	// 再加到索引
	b.indexer.Add(key, internal.NewItem(b.curr.FileID(), pos, size))
	return nil
}

func (b *BitCask) validKV(key, value []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}
	if b.config.MaxKeySize > 0 && uint32(len(key)) > b.config.MaxKeySize {
		return ErrKeyTooLarge
	}
	if b.config.MaxValueSize > 0 && uint64(len(value)) > b.config.MaxValueSize {
		return ErrValueTooLarge
	}
	return nil
}

func (b *BitCask) put(key, value []byte) (offset int64, size int, err error) {
	// 判断当前文件是否超出大小限制，需要关闭旧文件，创建新文件
	if b.isActiveFileExceedLimit() {
		err := b.curr.Close()
		if err != nil {
			return 0, 0, err
		}
		id := b.curr.FileID()
		oldDf, err := internal.NewBkFile(b.path, id, false)
		if err != nil {
			return 0, 0, err
		}
		b.dataFiles[id] = oldDf
		newDf, err := internal.NewBkFile(b.path, id+1, true)
		if err != nil {
			return 0, 0, err
		}
		b.curr = newDf
	}
	offset, size, err = b.curr.Write(internal.NewEntry(key, value))
	return
}

// Delete a key from a Bitcask datastore.
func (b *BitCask) Delete(key []byte) error {
	// 创建记录
	entry := internal.NewEntry(key, nil)
	// 写入磁盘
	_, _, err := b.curr.Write(entry)
	if err != nil {
		return err
	}
	// 内存索引中标记
	b.indexer.Delete(key)
	return nil
}

// ListKeys List all keys in a Bitcask datastore.
func (b *BitCask) ListKeys() [][]byte {
	return b.indexer.Keys()
}

// Fold over all K/V pairs in a Bitcask datastore.
// Fun is expected to be of the form: F(K,V,Acc0) → Acc.
func (b *BitCask) Fold(f func(key []byte) error) (err error) {
	b.lock.RLock()
	defer b.lock.RUnlock()
	for key := range b.indexer.Index() {
		kb := utils.Str2Bytes(key)
		if err := f(kb); err != nil {
			return err
		}
	}
	return
}

// merge Merge several data files within a Bitcask datastore into a more compact form.
// Also, produce hintfiles for faster startup.
func (b *BitCask) merge() error {
	b.lock.Lock()
	if b.isMerging {
		b.lock.Unlock()
		return ErrMergeInProgress
	}
	b.isMerging = true
	b.lock.Unlock()
	defer func() {
		b.isMerging = false
	}()
	b.lock.RLock()
	defer func() {
		b.lock.RUnlock()
	}()
	// 将当前活跃文件关闭
	err := b.closeActiveFile()
	if err != nil {
		return err
	}
	// 整理所有待合并的文件列表
	mergeFiles := make([]int, 0, len(b.dataFiles))
	for i := range b.dataFiles {
		mergeFiles = append(mergeFiles, i)
	}
	// 将当前的所有datafiles进行关闭
	// 创建一个新的file用于写操作
	err = b.newActiveFile()
	if err != nil {
		return err
	}
	sort.Ints(mergeFiles)
	lastMergeFile := mergeFiles[len(mergeFiles)-1]

	temp, err := ioutil.TempDir(b.path, "merge")
	if err != nil {
		return err
	}
	defer os.RemoveAll(temp)
	// 利用index，对所有关闭的datafiles进行遍历，写入到创建的临时db

	mergeDB, err := Open(temp, WithConfig(b.config))
	if err != nil {
		return err
	}
	err = b.Fold(func(key []byte) error {
		item, _ := b.indexer.Get(key)
		if item.FileID > lastMergeFile {
			return nil
		}
		val, err := b.Get(key)
		if err != nil {
			return err
		}
		err = mergeDB.Put(key, val)
		if err != nil {
			return err
		}
		return nil
	})
	if err = mergeDB.Close(); err != nil {
		return err
	}
	if err = b.Close(); err != nil {
		return err
	}
	return nil
}

// Sync Force any writes to sync to disk.
func (b *BitCask) Sync() error {
	return b.curr.Sync()
}

// Close a Bitcask data store and flush all pending writes (if any) to disk.
func (b *BitCask) Close() error {
	// 保存内存索引文件
	// 保存元数据、配置
	// 将归档文件落盘
	err := b.indexer.SaveToHintFile(b.path)
	if err != nil {
		return err
	}
	for _, file := range b.dataFiles {
		if err := file.Close(); err != nil {
			return err
		}
	}
	if err = b.curr.Close(); err != nil {
		return err
	}
	return nil
}

// isInActiveFile is query entry exist in active file
func (b *BitCask) isInActiveFile(id int) bool {
	return b.curr.FileID() == id
}

// isActiveFileExceedLimit is the size of active file exceed limit
func (b *BitCask) isActiveFileExceedLimit() bool {
	size, err := b.curr.Size()
	if err != nil {
		return true
	}
	return size >= b.config.MaxFileSize
}

// loadDataFiles 查找指定目录，读取所有已经记录的文件
func loadDataFiles(path string) (map[int]*internal.BkFile, int, error) {
	fns, err := utils.GetDataFiles(path)
	if err != nil {
		return nil, 0, err
	}
	fids, err := utils.GetFileIDs(fns)
	if err != nil {
		return nil, 0, err
	}
	var last int
	if len(fids) > 0 {
		last = fids[len(fids)-1]
	}
	datafiles := make(map[int]*internal.BkFile, len(fids))
	for _, fid := range fids {
		datafiles[fid], err = internal.NewBkFile(path, fid, false)
		if err != nil {
			return nil, 0, err
		}
	}
	return datafiles, last, nil
}

func loadIndexes(path string) (*internal.KeyDir, error) {
	idx := internal.NewKeyDir()
	path = filepath.Join(path, IndexFile)
	if !utils.Exist(path) {
		return idx, nil
	}
	hintf, err := os.Open(path)
	if err != nil {
		return idx, err
	}
	err = idx.ReloadFromHint(hintf)
	if err != nil {
		return idx, err
	}
	return idx, nil
}

// closeActiveFile 将当前活跃的文件关闭并加入旧文件列表
func (b *BitCask) closeActiveFile() error {
	if err := b.curr.Close(); err != nil {
		return err
	}
	id := b.curr.FileID()
	oldf, err := internal.NewBkFile(b.path, id, false)
	if err != nil {
		return err
	}
	b.dataFiles[id] = oldf
	return nil
}

// newActiveFile 打开一个新的活跃文件
func (b *BitCask) newActiveFile() error {
	activef, err := internal.NewBkFile(b.path, b.curr.FileID()+1, true)
	if err != nil {
		return err
	}
	b.curr = activef
	return nil
}
