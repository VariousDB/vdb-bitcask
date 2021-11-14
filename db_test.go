package bitcask

import (
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAll(t *testing.T) {
	var (
		db *BitCask
	)

	testDir, err := ioutil.TempDir("", "bitcask")
	assert.NoError(t, err)

	testDir = "data"
	t.Run("open", func(t *testing.T) {
		db, err = Open(testDir, WithMaxFileSize(1024))
		assert.NoError(t, err)
	})

	t.Run("get", func(t *testing.T) {
		val, err := db.Get([]byte("key"))
		assert.NoError(t, err)
		assert.Equal(t, []byte("value1"), val)
	})

	t.Run("put", func(t *testing.T) {
		err = db.Put([]byte("key1"), []byte("value1"))
		assert.NoError(t, err)
	})

	t.Run("get", func(t *testing.T) {
		val, err := db.Get([]byte("key1"))
		assert.NoError(t, err)
		assert.Equal(t, []byte("value1"), val)
	})

	t.Run("list", func(t *testing.T) {
		list := db.ListKeys()
		for _, bytes := range list {
			fmt.Println(string(bytes))
		}
	})

	t.Run("close", func(t *testing.T) {
		err = db.Close()
		assert.NoError(t, err)
	})
}
