package internal

import (
	"github.com/tiny-bitcask/utils"
	"sync"
	"time"
)

// Item is the index in memory
type Item struct {
	FileID    int   // specify which file
	ValueSize int64 // size of Value
	ValuePos  int64 // pos of Value for seek
	TimeStamp int64 // Timestamp
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
func NewItem(fileID int, pos, size int64) Item {
	return Item{
		FileID:    fileID,
		ValueSize: size,
		ValuePos:  pos,
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

func (k *KeyDir) Get(key []byte) (Item, bool) {
	keyStr := utils.Byte2Str(key)
	item, ok := k.index[keyStr]
	return item, ok
}

func (k *KeyDir) Delete() {

}
