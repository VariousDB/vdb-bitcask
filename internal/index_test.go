package internal

import (
	"fmt"
	"os"
	"testing"

	"github.com/go-playground/assert/v2"
)

func TestKeyDir_Write2Hint(t *testing.T) {
	kd := NewKeyDir()
	// hint, _ := os.OpenFile("data/index", os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	kd.Add([]byte("key1"), Item{
		FileID:    1,
		ValueSize: 20,
		ValuePos:  12,
		TimeStamp: 1234567,
	})
	kd.Add([]byte("key2"), Item{
		FileID:    2,
		ValueSize: 23,
		ValuePos:  11,
		TimeStamp: 1234567,
	})
	kd.Add([]byte("key3"), Item{
		FileID:    3,
		ValueSize: 54,
		ValuePos:  17,
		TimeStamp: 1234567,
	})
	kd.Add([]byte("key4"), Item{
		FileID:    4,
		ValueSize: 76,
		ValuePos:  45,
		TimeStamp: 1234567,
	})
	err := kd.SaveToHintFile("../data")
	assert.Equal(t, err, nil)

	newKd := NewKeyDir()
	h, _ := os.Open("../data/index")
	err = newKd.ReloadFromHint(h)
	if err != nil {
		t.Error(err)
		return
	}
	for s, item := range newKd.index {
		fmt.Printf("key:%v, value:%v\n", s, item)
	}
}
