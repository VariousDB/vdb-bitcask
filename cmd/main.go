package main

import (
	"fmt"
	bitcask "github.com/tiny-bitcask"
	"log"
)

func main() {
	db, err := bitcask.Open("", nil)
	if err != nil {
		return
	}
	err = db.Put([]byte("key"), []byte("value"))
	if err != nil {
		log.Fatalln(err)
	}
	val, err := db.Get([]byte("key"))
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println("value is:", val)
}
