package internal

import (
	"fmt"
	"github.com/go-playground/assert/v2"
	"testing"
)

func TestEntry_calcCheckSum(t *testing.T) {
	entry := NewEntry([]byte("key"), []byte("value"))
	fmt.Printf("%+v", entry)

	buf := entry.Encode()

	ne := Decode(buf)

	assert.Equal(t, ne, entry)
}
