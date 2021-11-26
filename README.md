# tiny-bitcask
Simple kv store engine inspired by bitcask

## Inspired By BitCask
[BitCask](./doc/bitcask-intro.pdf)

## System Design
1. DB启动时，系统内存储着`merged-data-file` 与 `hint-file`
> 存档只保存两个文件：data-file（存数据），hint-file（存索引）
2. 系统通过读取`hint-file`一次性拉取索引文件到内存中

> ``hint-file``结构：`timestamp | key-size | value-size | value-pos | key`

4. 初始化时创建一个active文件，用于存放新写入的kv对entry
5. PUT接口：写入entry时，先写磁盘再写内存哈希索引
6. 限定文件大小，当一个文件写满时，关闭此文件，创建新的活跃文件
7. 后台线程定期将`old-file`合并到`merged-data-file`，将内存索引覆盖重写`hint-file`
8. 当数据库关闭时，强制merge，保证系统中存放着两份文件（`bitcask.data` && `bitcask.hint`）
10. 如何合并`older-files`:遍历索引map，去除墓碑记录，挑选最新的记录，通过磁盘直接定位seek获取entry，写入合并后的新文件，并得到新的内存索引map
11. 每个文件开头设置标识位，如果已写满关闭的合法，因宕机未来的及归并的设置不合法
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
- [x] 完善内存哈希索引模块，在单个文件条件下测试 `GET/PUT` 接口
- [x] 增加`mode`字段 用来区分entry的操作类型
- [x] 拓展多文件，实现`older`、`active file`的区别
- [x] 实现后台`merge`功能，生成`merged-data-file` 与 `hint-file`
- [ ] 支持key过期