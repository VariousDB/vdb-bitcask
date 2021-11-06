package internal

import (
	"encoding/binary"
	"hash/crc32"
	"time"
)

const (
	EntryHeaderSize = 16
)

// Entry The format for each Key/Value entry
type Entry struct {
	// header
	CRC       uint32 // CRC checksum
	Timestamp uint32 // current Timestamp
	KeySize   uint32 // size of Key
	ValueSize uint32 // size of Value
	// payload
	Key   []byte // Key content
	Value []byte // Value content
}

// NewEntry return a format entry
func NewEntry(key, value []byte) *Entry {
	e := &Entry{
		Timestamp: uint32(time.Now().Unix()),
		KeySize:   uint32(len(key)),
		ValueSize: uint32(len(value)),
		Key:       key,
		Value:     value,
	}
	e.CRC = crc32.ChecksumIEEE(e.encodeWithoutCRC())
	return e
}

// encode without CRC
func (e *Entry) encodeWithoutCRC() []byte {
	buf := make([]byte, 12+len(e.Key)+len(e.Value))
	binary.LittleEndian.PutUint32(buf[0:4], e.Timestamp)
	binary.LittleEndian.PutUint32(buf[4:8], e.KeySize)
	binary.LittleEndian.PutUint32(buf[8:12], e.ValueSize)
	copy(buf[12:12+len(e.Key)], e.Key)
	copy(buf[12+len(e.Key):12+len(e.Key)+len(e.Value)], e.Value)
	return buf
}

// Encode entry to byte array
func (e *Entry) Encode() []byte {
	buf := make([]byte, EntryHeaderSize+len(e.Key)+len(e.Value))
	binary.LittleEndian.PutUint32(buf[0:4], e.CRC)
	copy(buf[4:], e.encodeWithoutCRC())
	return buf
}

// Decode byte array to Entry
func Decode(buf []byte) (entry *Entry) {
	entry = &Entry{}
	entry.CRC = binary.LittleEndian.Uint32(buf[0:4])
	entry.Timestamp = binary.LittleEndian.Uint32(buf[4:8])
	entry.KeySize = binary.LittleEndian.Uint32(buf[8:12])
	entry.ValueSize = binary.LittleEndian.Uint32(buf[12:16])
	entry.Key = buf[16 : 16+int(entry.KeySize)]
	entry.Value = buf[16+int(entry.KeySize) : 16+int(entry.KeySize)+int(entry.ValueSize)]
	return
}

func (e *Entry) Size() int64 {
	return int64(EntryHeaderSize + len(e.Key) + len(e.Value))
}

// IsValid Check if entry is valid
func (e *Entry) IsValid() bool {
	return e.CRC == crc32.ChecksumIEEE(e.encodeWithoutCRC())
}
