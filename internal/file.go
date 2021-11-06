package internal

import (
	"fmt"
	"os"
	"path/filepath"
)

const (
	DefaultBkFileName = "%v.data"
)

// BkFile in disk
type BkFile struct {
	id       int  // file id
	isActive bool // older or active file
	file     *os.File
}

// NewBkFile with new active file
func NewBkFile(path string, id int, active bool) (*BkFile, error) {
	fp := filepath.Join(path, fmt.Sprintf(DefaultBkFileName, id))
	var (
		f   *os.File
		err error
	)
	if !active {
		// open old file
		f, err = os.Open(fp)
		if err != nil {
			return nil, err
		}
	} else {
		f, err = os.OpenFile(fp, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0640)
		if err != nil {
			return nil, err
		}
	}
	bf := &BkFile{
		id:       id,
		file:     f,
		isActive: active,
	}
	return bf, nil
}

func (b *BkFile) Read(offset, size int64) (buf []byte, err error) {
	buf = make([]byte, size)
	_, err = b.file.ReadAt(buf, offset)
	return
}

func (b *BkFile) Write(entry *Entry) (pos int64, size int64) {
	stat, err := b.file.Stat()
	if err != nil {
		return
	}
	idx := stat.Size()
	_, err = b.file.WriteAt(entry.Encode(), idx)
	if err != nil {
		return
	}
	pos, size = idx, entry.Size()
	return
}

func (b *BkFile) FileID() int {
	return b.id
}
