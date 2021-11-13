package bitcask

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/zach030/tiny-bitcask/internal"
	"github.com/zach030/tiny-bitcask/utils"
)

var (
	ErrSpecifyKeyNotExist = errors.New("specify key not exist")
)

const (
	ArchivedDataFile = "bitcask"
	DataFileExt      = ".data"
	ArchivedHintFile = "bitcask.hint"
	HintFile         = ".hint"

	FirstActiveFile = 1
)

type DB struct {
	path    string
	indexer *internal.KeyDir
	curr    *internal.BkFile

	dataFiles map[int]*internal.BkFile
	dataFile  *os.File
	hintFile  *os.File

	maxSize int64
	mergeCh chan struct{}
}

// Open database
func Open(path string, cfg *Config) (*DB, error) {
	if cfg == nil {
		cfg = DefaultConfig
	}
	err := os.MkdirAll(path, 0700)
	if err != nil {
		return nil, err
	}
	db := &DB{
		path:    path,
		mergeCh: make(chan struct{}),
		maxSize: cfg.FileMaxSize,
	}
	err = db.rebuild()
	if err != nil {
		return nil, err
	}
	// go db.stat()
	return db, nil
}

// stat collect current db stat, send signal to merge and sync
func (d *DB) stat() {
	for {
		select {
		case <-d.mergeCh:
			d.merge()
		}
	}
}

// rebuild load from bitcask.hint file to build index
func (d *DB) rebuild() (err error) {
	dfs, last, err := loadDataFiles(d.path)
	if err != nil {
		return
	}
	idx, err := loadIndexes(d.path)
	if err != nil {
		return
	}
	curr, err := internal.NewBkFile(d.path, last, true)
	if err != nil {
		return
	}
	d.curr = curr
	d.indexer = idx
	d.dataFiles = dfs
	return
}

// Get Retrieve a value by key from a Bitcask datastore.
func (d *DB) Get(key []byte) ([]byte, error) {
	// 先从内存索引中获取此记录的信息，通过一次磁盘随机IO获取数据
	item, ok := d.indexer.Get(key)
	if !ok {
		return nil, ErrSpecifyKeyNotExist
	}
	// 读到的item所在文件可能是active和older
	if d.isInActiveFile(item.FileID) {
		return d.curr.Read(item.ValuePos, item.ValueSize)
	}
	// old-file
	// todo 临时对象new 使用优化
	bf, err := internal.NewBkFile(d.path, item.FileID, false)
	if err != nil {
		return nil, err
	}
	return bf.Read(item.ValuePos, item.ValueSize)
}

// Put Store a key and value in a Bitcask datastore.
func (d *DB) Put(key, value []byte) error {
	// 判断当前文件是否超出大小限制，需要关闭旧文件，创建新文件
	if d.isActiveFileExceedLimit() {
		newActiveFile, err := internal.NewBkFile(d.path, d.curr.FileID()+1, true)
		if err != nil {
			return err
		}
		d.curr = newActiveFile
	}
	// 创建记录，先写磁盘
	entry := internal.NewEntry(key, value)
	pos := d.curr.Write(entry)
	// 再加到索引
	d.indexer.Add(key, internal.NewItem(d.curr.FileID(), pos, entry))
	return nil
}

// Delete a key from a Bitcask datastore.
func (d *DB) Delete(key []byte) bool {
	// 创建记录
	entry := internal.NewEntry(key, nil)
	// 写入磁盘
	d.curr.Write(entry)
	// 内存索引中标记
	return d.indexer.Delete(key)
}

// ListKeys List all keys in a Bitcask datastore.
func (d *DB) ListKeys() [][]byte {
	return d.indexer.Keys()
}

// Fold over all K/V pairs in a Bitcask datastore.
// Fun is expected to be of the form: F(K,V,Acc0) → Acc.
func (d *DB) fold() {

}

// merge Merge several data files within a Bitcask datastore into a more compact form.
// Also, produce hintfiles for faster startup.
func (d *DB) merge() {
	//dfs, err := filepath.Glob(fmt.Sprintf("%s/*.data", d.path))
	//if err != nil {
	//	return
	//}
	//if len(dfs) == 0 {
	//	return
	//}
	//newDataFile, err := internal.NewBkFile(d.path, 0, true)
	//if err != nil {
	//	return
	//}
	//oldFiles := make([]*internal.BkFile, len(dfs))
	//for i, name := range dfs {
	//	name = strings.TrimPrefix(name, fmt.Sprintf("%s/", d.path))
	//	id, err := strconv.Atoi(strings.TrimSuffix(name, DataFileExt))
	//	if err != nil {
	//		continue
	//	}
	//	f, err := internal.NewBkFile(d.path, id, false)
	//	if err != nil {
	//		continue
	//	}
	//	oldFiles[i] = f
	//}
	// sync index to hint file
	//err = d.indexer.Write2Hint(d.hintFile)
	//if err != nil {
	//	return
	//}
	//newIdx := internal.NewKeyDir()
	//for key, item := range d.indexer.Index() {
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
	////d.dataFile = newDataFile
	//d.indexer = newIdx
}

// sync Force any writes to sync to disk.
func (d *DB) sync() bool {
	return true
}

// Close a Bitcask data store and flush all pending writes (if any) to disk.
func (d *DB) Close() bool {
	d.merge()
	return true
}

// isInActiveFile is query entry exist in active file
func (d *DB) isInActiveFile(id int) bool {
	return d.curr.FileID() == id
}

// isActiveFileExceedLimit is the size of active file exceed limit
func (d *DB) isActiveFileExceedLimit() bool {
	size, err := d.curr.Size()
	if err != nil {
		return true
	}
	return size >= d.maxSize
}

// loadDataFiles 查找指定目录，读取所有已经记录的文件
func loadDataFiles(path string) (datafiles map[int]*internal.BkFile, last int, err error) {
	fns, err := utils.GetDataFiles(path)
	if err != nil {
		return nil, 0, err
	}
	fids, err := utils.GetFileIDs(fns)
	if err != nil {
		return nil, 0, err
	}
	if len(fids) > 0 {
		last = fids[len(fids)-1]
	}
	datafiles = make(map[int]*internal.BkFile, len(fids))
	for _, fid := range fids {
		datafiles[fid], err = internal.NewBkFile(path, fid, false)
		if err != nil {
			return nil, 0, err
		}
	}
	return
}

func loadIndexes(path string) (*internal.KeyDir, error) {
	idx := internal.NewKeyDir()
	if !utils.Exist(filepath.Join(path, "index")) {
		return nil, nil
	}
	hintf, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	err = idx.ReloadFromHint(hintf)
	if err != nil {
		return nil, err
	}
	return idx, nil
}
