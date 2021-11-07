package bitcask

var DefaultConfig = &Config{
	FileMaxSize: 2 << 10,
}

type Config struct {
	FileMaxSize int64 // 每个文件最大值
}
