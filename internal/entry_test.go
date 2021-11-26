package internal

import (
	"testing"

	"github.com/go-playground/assert/v2"
)

func TestEntry(t *testing.T) {
	t.Run("encode and decode", func(t *testing.T) {
		entry := NewEntry([]byte("key"), []byte("value"))
		buf := entry.Encode()
		ne := Decode(buf)
		assert.Equal(t, ne, entry)
	})

	t.Run("valid entry", func(t *testing.T) {
		entry := NewEntry([]byte("key"), []byte("value"))
		entry.value = []byte("value2")
		assert.Equal(t, false, entry.IsValid())
	})
}
