package bitcask

import "errors"

var (
	ErrSpecifyKeyNotExist = errors.New("specify key not exist")
	ErrEmptyKey           = errors.New("empty key")
	ErrKeyTooLarge        = errors.New("key too large")
	ErrValueTooLarge      = errors.New("value too large")
	ErrInvalidCheckSum    = errors.New("invalid checksum")

	ErrMergeInProgress = errors.New("database is in merge progress")
)
