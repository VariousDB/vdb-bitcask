package bitcask

import (
	"errors"
	"os"
	"path/filepath"
	"sync"

	"github.com/zach030/tiny-bitcask/internal"
	"github.com/zach030/tiny-bitcask/utils"
)

var (
	ErrSpecifyKeyNotExist = errors.New("specify key not exist")
	ErrEmptyKey           = errors.New("empty key")
	ErrKeyTooLarge        = errors.New("key too large")
	ErrValueTooLarge      = errors.New("value too large")
	ErrInvalidCheckSum    = errors.New("invalid checksum")
)

const (
	ArchivedDataFile = "bitcask"
	DataFileExt      = ".data"
	IndexFile        = "index"

	FirstActiveFile = 1
)

type BitCask struct {
	path string
	lock sync.RWMutex

	indexer *internal.KeyDir
	curr    *internal.BkFile

	dataFiles map[int]*internal.BkFile

	config *Config
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
func (b *BitCask) merge() {
	//dfs, err := filepath.Glob(fmt.Sprintf("%s/*.data", b.path))
	//if err != nil {
	//	return
	//}
	//if len(dfs) == 0 {
	//	return
	//}
	//newDataFile, err := internal.NewBkFile(b.path, 0, true)
	//if err != nil {
	//	return
	//}
	//oldFiles := make([]*internal.BkFile, len(dfs))
	//for i, name := range dfs {
	//	name = strings.TrimPrefix(name, fmt.Sprintf("%s/", b.path))
	//	id, err := strconv.Atoi(strings.TrimSuffix(name, DataFileExt))
	//	if err != nil {
	//		continue
	//	}
	//	f, err := internal.NewBkFile(b.path, id, false)
	//	if err != nil {
	//		continue
	//	}
	//	oldFiles[i] = f
	//}
	// sync index to hint file
	//err = b.indexer.SaveToHintFile(b.hintFile)
	//if err != nil {
	//	return
	//}
	//newIdx := internal.NewKeyDir()
	//for key, item := range b.indexer.Index() {
	//	oldF := oldFiles[item.FileID]
	//	val, err := oldF.Read(item.ValuePos, item.ValueSize)
	//	if err != nil {
	//		continue
	//	}
	//	entry := internal.NewEntry(utils.Str2Bytes(key), val)
	//	offset := newDataFile.Write(entry)
	//	newIdx.Add(utils.Str2Bytes(key), internal.NewItem())
	//}
	//os.Rename(newDataFile.Name(), ArchivedDataFile)
	//for _, file := range oldFiles {
	//	os.Remove(file.Name())
	//}
	////b.dataFile = newDataFile
	//b.indexer = newIdx
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
