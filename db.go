package bitcask

import (
	"errors"
	"github.com/tiny-bitcask/internal"
	"os"
)

var (
	ErrSpecifyKeyNotExist = errors.New("specify key not exist")
)

type DB struct {
	path       string
	idx        *internal.KeyDir
	activeFile *internal.BkFile
}

// Open database
func Open(path string, cfg *Config) (*DB, error) {
	err := os.MkdirAll(path, 0700)
	if err != nil {
		return nil, err
	}
	return &DB{
		path: path,
		idx:  internal.NewKeyDir(),
	}, nil
}

// Get Retrieve a value by key from a Bitcask datastore.
func (d *DB) Get(key []byte) (val []byte, err error) {
	// 先从内存索引中获取此记录的信息，通过一次磁盘随机IO获取数据
	item, ok := d.idx.Get(key)
	if !ok {
		err = ErrSpecifyKeyNotExist
		return
	}
	// 读到的item所在文件可能是active和older
	// 如果是active-file，则调用activeFile
	if d.isInActiveFile(item.FileID) {
		val, err = d.activeFile.Read(item.ValuePos, item.ValueSize)
		return
	}
	// old-file
	bf, err := internal.NewBkFile(d.path, item.FileID, false)
	if err != nil {
		return
	}
	val, err = bf.Read(item.ValuePos, item.ValueSize)
	return
}

// Put Store a key and value in a Bitcask datastore.
func (d *DB) Put(key, value []byte) error {
	// 先写磁盘
	entry := internal.NewEntry(key, value)
	pos, size := d.activeFile.Write(entry)
	// 再加到索引
	d.idx.Add(key, internal.NewItem(d.activeFile.FileID(), pos, size))
	return nil
}

// Delete a key from a Bitcask datastore.
func (d *DB) Delete(key []byte) error {
	return nil
}

// ListKeys List all keys in a Bitcask datastore.
func (d *DB) ListKeys() [][]byte {
	return nil
}

// Fold over all K/V pairs in a Bitcask datastore.
// Fun is expected to be of the form: F(K,V,Acc0) → Acc.
func (d *DB) fold() {

}

// merge Merge several data files within a Bitcask datastore into a more compact form.
// Also, produce hintfiles for faster startup.
func (d *DB) merge(dir string) bool {
	return true
}

// sync Force any writes to sync to disk.
func (d *DB) sync() bool {
	return true
}

// Close a Bitcask data store and flush all pending writes (if any) to disk.
func (d *DB) Close() bool {
	return true
}

func (d *DB) isInActiveFile(id int) bool {
	return d.activeFile.FileID() == id
}
