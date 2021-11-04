# tiny-bitcask
simple kv store engine inspired by bitcask

## Inspired By BitCask
[BitCask](./doc/bitcask-intro.pdf)

## System Design

## DataBase API Design
```go
// Open database instance
func Open(dir string, cfg *Config)*DB
// Get Retrieve a value by key from a Bitcask datastore.
func (d *DB) Get(key []byte) ([]byte, error)
// Put Store a key and value in a Bitcask datastore.
func (d *DB) Put(key, value []byte)error
// Delete a key from a Bitcask datastore.
func (d *DB) Delete(key []byte)error
// ListKeys List all keys in a Bitcask datastore.
func (d *DB) ListKeys()[][]byte
// Fold over all K/V pairs in a Bitcask datastore.
// Fun is expected to be of the form: F(K,V,Acc0) → Acc.
func (d *DB) fold()
// merge Merge several data files within a Bitcask datastore into a more compact form.
// Also, produce hintfiles for faster startup.
func (d *DB) merge(dir string) bool
// sync Force any writes to sync to disk.
func (d *DB) sync() bool
// Close a Bitcask data store and flush all pending writes (if any) to disk.
func (d *DB) Close() bool
```

## TODO-LIST
- [ ] 完善内存哈希索引模块，在单个文件条件下测试 `GET/PUT` 接口
- [ ] 增加`mode`字段 用来区分entry的操作类型
- [ ] 拓展多文件，实现`older`、`active file`的区别
- [ ] 实现后台`merge`功能，生成`merged-data-file` 与 `hint-file`