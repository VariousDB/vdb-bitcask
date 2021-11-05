package internal

import (
	"github.com/tiny-bitcask/utils"
	"sync"
)

// Item is the index in memory
type Item struct {
	FileID    uint32
	ValueSize uint32
	ValuePos  uint32
	TimeStamp uint32
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

func (k *KeyDir) Add(key []byte, fileID int) {
	k.lock.Lock()
	defer k.lock.Unlock()

	keyStr := utils.Byte2Str(key)
	k.index[keyStr] = Item{
		FileID:    0,
		ValueSize: 0,
		ValuePos:  0,
		TimeStamp: 0,
	}
}

func (k *KeyDir) Get() {

}

func (k *KeyDir) Delete() {

}
