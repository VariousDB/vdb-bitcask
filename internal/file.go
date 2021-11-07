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
	id       int      // file id
	isActive bool     // older or active file
	rf       *os.File // read file
	wf       *os.File // write file
}

// NewBkFile with new active file
func NewBkFile(path string, id int, active bool) (*BkFile, error) {
	fp := filepath.Join(path, fmt.Sprintf(DefaultBkFileName, id))
	var (
		wf  *os.File
		err error
	)
	if active {
		wf, err = os.OpenFile(fp, os.O_WRONLY|os.O_CREATE, 0640)
		if err != nil {
			return nil, err
		}
	}
	rf, err := os.Open(fp)
	if err != nil {
		return nil, err
	}
	bf := &BkFile{
		id:       id,
		rf:       rf,
		wf:       wf,
		isActive: active,
	}
	return bf, nil
}

func (b *BkFile) Read(offset, size int64) (buf []byte, err error) {
	buf = make([]byte, size)
	_, err = b.rf.ReadAt(buf, offset)
	return
}

func (b *BkFile) Write(entry *Entry) (pos int64) {
	stat, err := b.wf.Stat()
	if err != nil {
		return
	}
	idx := stat.Size()
	_, err = b.wf.WriteAt(entry.Encode(), idx)
	if err != nil {
		return
	}
	pos = idx
	return
}

func (b *BkFile) FileID() int {
	return b.id
}
