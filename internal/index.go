package internal

import (
	"bytes"
	"encoding/gob"
	"github.com/zach030/tiny-bitcask/utils"
	"io"
	"sync"
	"time"
)

// Item is the index in memory
type Item struct {
	FileID    int   // specify which file
	ValueSize int64 // size of value
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
func NewItem(fileID int, pos int64, entry *Entry) Item {
	return Item{
		FileID:    fileID,
		ValueSize: entry.ValueSize(),
		ValuePos:  pos + entry.ValueOffset(),
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
	keyStr := utils.Byte2Str(key)
	item, ok := k.index[keyStr]
	return item, ok
}

// Delete item in index
func (k *KeyDir) Delete(key []byte) bool {
	k.lock.Lock()
	defer k.lock.Unlock()
	keyStr := utils.Byte2Str(key)
	_, ok := k.index[keyStr]
	if !ok {
		return ok
	}
	delete(k.index, keyStr)
	return true
}

// Keys list all keys in index
func (k *KeyDir) Keys() [][]byte {
	keys := make([][]byte, 0)
	for key, _ := range k.index {
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

// Write2Hint save key-dirs hash index to hint-file
func (k *KeyDir) Write2Hint(w io.Writer) (err error) {
	buf, err := k.Encode()
	if err != nil {
		return err
	}
	_, err = w.Write(buf)
	if err != nil {
		return
	}
	return
}

// ReloadFromHint load key-dirs index from file
func (k *KeyDir) ReloadFromHint(r io.Reader) error {
	dec := gob.NewDecoder(r)
	return dec.Decode(&k.index)
}
