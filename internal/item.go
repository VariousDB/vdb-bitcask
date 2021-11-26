package internal

// Item is the index in memory
type Item struct {
	FileID    int   // specify which datafile
	ValueSize int   // size of value
	ValuePos  int64 // pos of value for seek
	TimeStamp int64 // timestamp
}
