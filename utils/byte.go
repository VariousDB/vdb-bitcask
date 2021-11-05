package utils

import "unsafe"

// refer https://www.cnblogs.com/shuiyuejiangnan/p/9707066.html

// Byte2Str convert []byte to string
func Byte2Str(buf []byte) string {
	return *(*string)(unsafe.Pointer(&buf))
}

// Str2Bytes convert string to []byte
func Str2Bytes(s string) []byte {
	x := (*[2]uintptr)(unsafe.Pointer(&s))
	h := [3]uintptr{x[0], x[1], x[1]}
	return *(*[]byte)(unsafe.Pointer(&h))
}
