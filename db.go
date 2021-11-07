package bitcask

import (
	"errors"
	"fmt"
	"github.com/zach030/tiny-bitcask/internal"
	"github.com/zach030/tiny-bitcask/utils"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

var (
	ErrSpecifyKeyNotExist = errors.New("specify key not exist")
)

const (
	ArchivedDataFile = "bitcask.data"
	DataFileExt      = ".data"
	ArchivedHintFile = "bitcask.hint"
	HintFile         = ".hint"
)

type DB struct {
	path       string
	idx        *internal.KeyDir
	activeFile *internal.BkFile

	dataFile *os.File
	hintFile *os.File

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
		idx:     internal.NewKeyDir(),
		mergeCh: make(chan struct{}),
		maxSize: cfg.FileMaxSize,
	}
	err = db.rebuild()
	if err != nil {
		return nil, err
	}
	f, err := internal.NewBkFile(path, 1, true)
	if err != nil {
		return nil, err
	}
	db.activeFile = f
	go db.stat()
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
func (d *DB) rebuild() error {
	// 如果有索引文件，则需要读取文件，重建内存索引
	hintFile, err := os.Open(ArchivedHintFile)
	if err != nil && err != os.ErrNotExist {
		return err
	}
	d.hintFile = hintFile
	dataFile, err := os.Open(ArchivedDataFile)
	if err != nil && err != os.ErrNotExist {
		return err
	}
	d.dataFile = dataFile
	return d.idx.ReloadFromHint(d.hintFile)
}

// Get Retrieve a value by key from a Bitcask datastore.
func (d *DB) Get(key []byte) ([]byte, error) {
	// 先从内存索引中获取此记录的信息，通过一次磁盘随机IO获取数据
	item, ok := d.idx.Get(key)
	if !ok {
		return nil, ErrSpecifyKeyNotExist
	}
	// 读到的item所在文件可能是active和older
	if d.isInActiveFile(item.FileID) {
		return d.activeFile.Read(item.ValuePos, item.ValueSize)
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
		newActiveFile, err := internal.NewBkFile(d.path, d.activeFile.FileID()+1, true)
		if err != nil {
			return err
		}
		d.activeFile = newActiveFile
	}
	// 创建记录，先写磁盘
	entry := internal.NewEntry(key, value)
	pos := d.activeFile.Write(entry)
	// 再加到索引
	d.idx.Add(key, internal.NewItem(d.activeFile.FileID(), pos, entry))
	return nil
}

// Delete a key from a Bitcask datastore.
func (d *DB) Delete(key []byte) bool {
	// 创建记录
	entry := internal.NewEntry(key, nil)
	// 写入磁盘
	d.activeFile.Write(entry)
	// 内存索引中标记
	return d.idx.Delete(key)
}

// ListKeys List all keys in a Bitcask datastore.
func (d *DB) ListKeys() [][]byte {
	return d.idx.Keys()
}

// Fold over all K/V pairs in a Bitcask datastore.
// Fun is expected to be of the form: F(K,V,Acc0) → Acc.
func (d *DB) fold() {

}

// merge Merge several data files within a Bitcask datastore into a more compact form.
// Also, produce hintfiles for faster startup.
func (d *DB) merge() {
	dfs, err := filepath.Glob(fmt.Sprintf("%s/*.data", d.path))
	if err != nil {
		return
	}
	if len(dfs) <= 1 {
		return
	}
	oldFiles := make([]*internal.BkFile, len(dfs))
	for i, name := range dfs {
		id, err := strconv.Atoi(strings.TrimSuffix(name, ".data"))
		if err != nil {

		}
		f, err := internal.NewBkFile(d.path, id, false)
		if err != nil {

		}
		oldFiles[i] = f
	}
	// sync index to hint file
	err = d.idx.Write2Hint(d.hintFile)
	if err != nil {
		return
	}
	for key, item := range d.idx.Index() {
		oldF := oldFiles[item.FileID]
		val, err := oldF.Read(item.ValuePos, item.ValueSize)
		if err != nil {

		}
		entry := internal.NewEntry(utils.Str2Bytes(key), val)
		d.dataFile.Write(entry.Encode())
	}
}

// sync Force any writes to sync to disk.
func (d *DB) sync() bool {
	return true
}

// Close a Bitcask data store and flush all pending writes (if any) to disk.
func (d *DB) Close() bool {
	return true
}

// isInActiveFile is query entry exist in active file
func (d *DB) isInActiveFile(id int) bool {
	return d.activeFile.FileID() == id
}

// isActiveFileExceedLimit is the size of active file exceed limit
func (d *DB) isActiveFileExceedLimit() bool {
	size, err := d.activeFile.Size()
	if err != nil {
		return true
	}
	return size >= d.maxSize
}
