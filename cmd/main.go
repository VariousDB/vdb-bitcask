package main

import (
	"fmt"
	bitcask "github.com/zach030/tiny-bitcask"
	"log"
)

func main() {
	db, err := bitcask.Open("data", nil)
	defer db.Close()
	if err != nil {
		return
	}
	err = db.Put([]byte("key1"), []byte("value"))
	if err != nil {
		log.Fatalln(err)
	}
	val, err := db.Get([]byte("key1"))
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println("value is:", string(val))
}
