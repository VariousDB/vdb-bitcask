package internal

type Index map[string]KeyDir

// KeyDir is the index in memory
type KeyDir struct {
	FileID    uint32
	ValueSize uint32
	ValuePos  uint32
	TimeStamp uint32
}
