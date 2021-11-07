package internal

import (
	"encoding/binary"
	"hash/crc32"
	"time"
)

const (
	EntryHeaderSize = 20
)

// Entry The format for each key/value entry
type Entry struct {
	// header
	crc       uint32 // crc checksum
	timestamp int64  // current timestamp
	keySize   uint32 // size of key
	valueSize uint32 // size of value
	// payload
	key   []byte // key content
	value []byte // value content
}

// NewEntry return a format entry
func NewEntry(key, value []byte) *Entry {
	e := &Entry{
		timestamp: time.Now().Unix(),
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
	buf := make([]byte, 16+len(e.key)+len(e.value))
	binary.LittleEndian.PutUint64(buf[0:8], uint64(e.timestamp))
	binary.LittleEndian.PutUint32(buf[8:12], e.keySize)
	binary.LittleEndian.PutUint32(buf[12:16], e.valueSize)
	copy(buf[16:16+len(e.key)], e.key)
	copy(buf[16+len(e.key):16+len(e.key)+len(e.value)], e.value)
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
	entry.timestamp = int64(binary.LittleEndian.Uint64(buf[4:12]))
	entry.keySize = binary.LittleEndian.Uint32(buf[12:16])
	entry.valueSize = binary.LittleEndian.Uint32(buf[16:20])
	entry.key = buf[EntryHeaderSize : EntryHeaderSize+int(entry.keySize)]
	entry.value = buf[EntryHeaderSize+int(entry.keySize) : EntryHeaderSize+int(entry.keySize)+int(entry.valueSize)]
	return
}

func (e *Entry) ValueOffset() int64 {
	return int64(EntryHeaderSize + len(e.key))
}

func (e *Entry) ValueSize() int64 {
	return int64(e.valueSize)
}

func (e *Entry) Size() int64 {
	return int64(EntryHeaderSize + len(e.key) + len(e.value))
}

// IsValid Check if entry is valid
func (e *Entry) IsValid() bool {
	return e.crc == crc32.ChecksumIEEE(e.encodeWithoutCRC())
}
