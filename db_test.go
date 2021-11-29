package bitcask

import (
	"fmt"
	"io/ioutil"
	"math/rand"
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

	t.Run("put", func(t *testing.T) {
		err = db.Put([]byte("key"), []byte("value"))
		assert.NoError(t, err)
	})

	t.Run("get", func(t *testing.T) {
		val, err := db.Get([]byte("key"))
		assert.NoError(t, err)
		assert.Equal(t, []byte("value"), val)
	})

	t.Run("put", func(t *testing.T) {
		err = db.Put([]byte("key1"), []byte("value1"))
		assert.NoError(t, err)
	})

	t.Run("get-exist", func(t *testing.T) {
		val, err := db.Get([]byte("key1"))
		assert.NoError(t, err)
		assert.Equal(t, []byte("value1"), val)
	})

	t.Run("batch put", func(t *testing.T) {
		s := rand.Int()
		e := rand.Intn(30)
		for i := s; i < s+e; i++ {
			err := db.Put([]byte(fmt.Sprintf("key%v", i)), []byte(fmt.Sprintf("squre value:%v", i*i)))
			assert.Nil(t, err)
		}
	})

	t.Run("list-test", func(t *testing.T) {
		list := db.ListKeys()
		fmt.Println(list)
	})

	t.Run("merge", func(t *testing.T) {
		err = db.merge()
		assert.NoError(t, err)
		assert.Equal(t, int64(0), db.metadata.ReclaimSpace)
	})

	t.Run("close", func(t *testing.T) {
		err = db.Close()
		assert.NoError(t, err)
	})
}
