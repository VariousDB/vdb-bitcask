package internal

import (
	"bytes"
	"encoding/gob"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/zach030/tiny-bitcask/utils"
)

// Item is the index in memory
type Item struct {
	FileID    int   // specify which file
	ValueSize int   // size of value
	ValuePos  int64 // pos of value for seek
	TimeStamp int64 // timestamp
}

// KeyDir the index in memory
type KeyDir struct {
	lock  sync.RWMutex
	index map[string]Item
}

// NewKeyDir returns memory index
func NewKeyDir() *KeyDir {
	return &KeyDir{
		index: make(map[string]Item),
	}
}

// NewItem return new item
func NewItem(fileID int, offset int64, size int) Item {
	return Item{
		FileID:    fileID,
		ValueSize: size,
		ValuePos:  offset,
		TimeStamp: time.Now().Unix(),
	}
}

// Add idx to memory after write in disk
func (k *KeyDir) Add(key []byte, item Item) {
	k.lock.Lock()
	defer k.lock.Unlock()
	keyStr := utils.Byte2Str(key)
	k.index[keyStr] = item
}

// Get item in index
func (k *KeyDir) Get(key []byte) (Item, bool) {
	k.lock.RLock()
	defer k.lock.RUnlock()
	keyStr := utils.Byte2Str(key)
	item, ok := k.index[keyStr]
	return item, ok
}

// Delete item in index
func (k *KeyDir) Delete(key []byte) {
	k.lock.Lock()
	defer k.lock.Unlock()
	keyStr := utils.Byte2Str(key)
	_, ok := k.index[keyStr]
	if !ok {
		return
	}
	delete(k.index, keyStr)
}

// Keys list all keys in index
func (k *KeyDir) Keys() [][]byte {
	k.lock.RLock()
	defer k.lock.RUnlock()
	keys := make([][]byte, 0)
	for key := range k.index {
		keys = append(keys, utils.Str2Bytes(key))
	}
	return keys
}

// Index map in key-dir
func (k *KeyDir) Index() map[string]Item {
	return k.index
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

// SaveToHintFile save key-dirs hash index to hint-file
func (k *KeyDir) SaveToHintFile(path string) (err error) {
	tmpExt := "index-tmp"
	tmpPath := filepath.Join(path, tmpExt)
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

// ReloadFromHint load key-dirs index from file
func (k *KeyDir) ReloadFromHint(r io.Reader) error {
	dec := gob.NewDecoder(r)
	return dec.Decode(&k.index)
}
