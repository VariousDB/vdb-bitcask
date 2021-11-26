package index

import (
	"bytes"
	"encoding/gob"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/zach030/tiny-bitcask/internal"
	"github.com/zach030/tiny-bitcask/utils"
)

type Index interface {
	Add([]byte, internal.Item)
	Get([]byte) (internal.Item, bool)
	Has([]byte) bool
	Delete([]byte)
	Keys() []string
	Encode() ([]byte, error)
	Sync(string) error
	Load(io.Reader) error
	Index() map[string]internal.Item
}

// KeyDir the index in memory
type KeyDir struct {
	sync.RWMutex
	index map[string]internal.Item
}

// NewKeyDir returns memory index
func NewKeyDir() Index {
	return &KeyDir{
		index: make(map[string]internal.Item),
	}
}

// NewItem return new item
func NewItem(fileID int, offset int64, size int) internal.Item {
	return internal.Item{
		FileID:    fileID,
		ValueSize: size,
		ValuePos:  offset,
		TimeStamp: time.Now().Unix(),
	}
}

// Add idx to memory after write in disk
func (k *KeyDir) Add(key []byte, item internal.Item) {
	k.Lock()
	defer k.Unlock()
	keyStr := utils.Byte2Str(key)
	k.index[keyStr] = item
}

// Get item in index
func (k *KeyDir) Get(key []byte) (internal.Item, bool) {
	k.RLock()
	defer k.RUnlock()
	keyStr := utils.Byte2Str(key)
	item, ok := k.index[keyStr]
	return item, ok
}

// Has item in index
func (k *KeyDir) Has(key []byte) bool {
	k.RLock()
	defer k.RUnlock()
	keyStr := utils.Byte2Str(key)
	_, ok := k.index[keyStr]
	return ok
}

// Delete item in index
func (k *KeyDir) Delete(key []byte) {
	k.Lock()
	defer k.Unlock()
	keyStr := utils.Byte2Str(key)
	_, ok := k.index[keyStr]
	if !ok {
		return
	}
	delete(k.index, keyStr)
}

// Keys list all keys in index
func (k *KeyDir) Keys() []string {
	k.RLock()
	defer k.RUnlock()
	keys := make([]string, 0)
	for key := range k.index {
		keys = append(keys, key)
	}
	return keys
}

// Index map in key-dir
func (k *KeyDir) Index() map[string]internal.Item {
	k.RLock()
	defer k.RUnlock()
	idx := make(map[string]internal.Item)
	for key, item := range k.index {
		idx[key] = item
	}
	return idx
}

func (k *KeyDir) Encode() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(k.index)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Sync save key-dirs hash index to hint-datafile
func (k *KeyDir) Sync(path string) (err error) {
	tmpPath := filepath.Join(path, "index-temp")
	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	buf, err := k.Encode()
	if err != nil {
		return err
	}
	if _, err = f.Write(buf); err != nil {
		return
	}
	if err = f.Sync(); err != nil {
		return
	}
	if err = os.Rename(tmpPath, filepath.Join(path, "index")); err != nil {
		return err
	}
	return
}

// Load key-dirs index from datafile
func (k *KeyDir) Load(r io.Reader) error {
	dec := gob.NewDecoder(r)
	return dec.Decode(&k.index)
}
