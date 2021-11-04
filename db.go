package main

type DB struct {
	
}

func Open(dir string, cfg *Config)*DB  {
	return &DB{}
}

// Get Retrieve a value by key from a Bitcask datastore.
func (d *DB) Get(key []byte)  {

}

// Put Store a key and value in a Bitcask datastore.
func (d *DB) Put(key,value []byte)  {

}

// Delete a key from a Bitcask datastore.
func (d *DB) Delete(key []byte) {

}

// ListKeys List all keys in a Bitcask datastore.
func (d *DB) ListKeys() {

}

// Fold over all K/V pairs in a Bitcask datastore.
// Fun is expected to be of the form: F(K,V,Acc0) → Acc.
func (d *DB) fold() {

}

// merge Merge several data files within a Bitcask datastore into a more compact form.
// Also, produce hintfiles for faster startup.
func (d *DB) merge(dir string)bool {
	return true
}

// sync Force any writes to sync to disk.
func (d *DB) sync()bool  {
	return true
}

// Close a Bitcask data store and flush all pending writes (if any) to disk.
func (d *DB) Close() bool{
	return true
}