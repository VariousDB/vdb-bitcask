package main

import (
	"fmt"
	bitcask "github.com/zach030/tiny-bitcask"
	"log"
)

func main() {
	db, err := bitcask.Open("data", nil)
	if err != nil {
		return
	}
	err = db.Put([]byte("key"), []byte("value"))
	if err != nil {
		log.Fatalln(err)
	}
	val, err := db.Get([]byte("key1"))
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println("value is:", string(val))
}
