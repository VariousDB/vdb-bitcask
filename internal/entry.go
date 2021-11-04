package internal

import (
	"encoding/binary"
	"hash/crc32"
	"time"
)

const (
	EntryHeaderSize = 16
)

// Entry The format for each key/value entry
type Entry struct {
	// header
	crc       uint32 // crc checksum
	timestamp uint32 // current timestamp
	keySize   uint32 // size of key
	valueSize uint32 // size of value
	// payload
	key   []byte // key content
	value []byte // value content
}

// NewEntry return a format entry
func NewEntry(key, value []byte) *Entry {
	e := &Entry{
		timestamp: uint32(time.Now().Unix()),
		keySize:   uint32(len(key)),
		valueSize: uint32(len(value)),
		key:       key,
		value:     value,
	}
	e.crc = crc32.ChecksumIEEE(e.encodeWithoutCRC())
	return e
}

// encode without crc
func (e *Entry) encodeWithoutCRC() []byte {
	buf := make([]byte, 12+len(e.key)+len(e.value))
	binary.LittleEndian.PutUint32(buf[0:4], e.timestamp)
	binary.LittleEndian.PutUint32(buf[4:8], e.keySize)
	binary.LittleEndian.PutUint32(buf[8:12], e.valueSize)
	copy(buf[12:12+len(e.key)], e.key)
	copy(buf[12+len(e.key):12+len(e.key)+len(e.value)], e.value)
	return buf
}

// Encode entry to byte array
func (e *Entry) Encode() []byte {
	buf := make([]byte, EntryHeaderSize+len(e.key)+len(e.value))
	binary.LittleEndian.PutUint32(buf[0:4], e.crc)
	copy(buf[4:], e.encodeWithoutCRC())
	return buf
}

// Decode byte array to Entry
func Decode(buf []byte) (entry *Entry) {
	entry = &Entry{}
	entry.crc = binary.LittleEndian.Uint32(buf[0:4])
	entry.timestamp = binary.LittleEndian.Uint32(buf[4:8])
	entry.keySize = binary.LittleEndian.Uint32(buf[8:12])
	entry.valueSize = binary.LittleEndian.Uint32(buf[12:16])
	entry.key = buf[16 : 16+int(entry.keySize)]
	entry.value = buf[16+int(entry.keySize) : 16+int(entry.keySize)+int(entry.valueSize)]
	return
}
