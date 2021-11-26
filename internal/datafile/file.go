package datafile

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/zach030/tiny-bitcask/internal"
)

const (
	DefaultBkFileName = "%v.data"
)

var (
	ErrReadOnlyFile = errors.New("readonly data-datafile")
)

type DataFile interface {
	Read(offset int64, size int) (*internal.Entry, error) // read entry
	Write(entry *internal.Entry) (int64, int, error)      // write entry
	FileID() int                                          // get datafile id
	Size() int64                                          // get datafile size
	Name() string                                         // get datafile name
	Close() error                                         // close datafile
	Sync() error                                          // sync datafile to disk
}

// BkFile in disk
type BkFile struct {
	sync.RWMutex
	id       int      // datafile id
	isActive bool     // older or active datafile
	rf       *os.File // read datafile
	wf       *os.File // write datafile
	offset   int64
}

// NewBkFile with new active datafile
func NewBkFile(path string, id int, active bool) (DataFile, error) {
	fp := filepath.Join(path, fmt.Sprintf(DefaultBkFileName, id))
	var (
		rf  *os.File
		wf  *os.File
		err error
	)
	if active {
		if wf, err = os.OpenFile(fp, os.O_WRONLY|os.O_CREATE, 0640); err != nil {
			return nil, err
		}
	}
	if rf, err = os.Open(fp); err != nil {
		return nil, err
	}
	stat, err := os.Stat(rf.Name())
	if err != nil {
		return nil, err
	}
	bf := &BkFile{
		id:       id,
		rf:       rf,
		wf:       wf,
		offset:   stat.Size(),
		isActive: active,
	}
	return bf, nil
}

// Read buf from files: older or active
func (b *BkFile) Read(offset int64, size int) (entry *internal.Entry, err error) {
	buf := make([]byte, size)
	_, err = b.rf.ReadAt(buf, offset)
	entry = internal.Decode(buf)
	return
}

// Write entry to active datafile
func (b *BkFile) Write(entry *internal.Entry) (int64, int, error) {
	b.Lock()
	defer b.Unlock()
	if b.wf == nil {
		return -1, 0, ErrReadOnlyFile
	}
	offset := b.offset
	n, err := b.wf.WriteAt(entry.Encode(), offset)
	if err != nil {
		return -1, 0, err
	}
	b.offset += int64(n)
	return offset, n, nil
}

// FileID get current datafile id
func (b *BkFile) FileID() int {
	return b.id
}

// Size get current datafile size, required when write
func (b *BkFile) Size() int64 {
	b.RLock()
	defer b.RUnlock()
	return b.offset
}

func (b *BkFile) Name() string {
	if b.wf != nil {
		return b.wf.Name()
	}
	return b.rf.Name()
}

func (b *BkFile) Close() error {
	defer func() {
		if b.rf != nil {
			b.rf.Close()
		}
	}()
	if b.wf == nil {
		return nil
	}
	err := b.wf.Sync()
	if err != nil {
		return err
	}
	return b.wf.Close()
}

func (b *BkFile) Sync() error {
	if b.wf == nil {
		return nil
	}
	return b.wf.Sync()
}
