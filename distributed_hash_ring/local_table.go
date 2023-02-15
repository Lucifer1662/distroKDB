package distributed_hash_ring

import (
	"luke/distrokdb/hash_ring"
)

type LocalTable struct {
	table hash_ring.KeyValueTable
}

func (t *LocalTable) Add(key string, value string) error {
	return t.table.Add(key, value)
}

func (t *LocalTable) Get(key string) (*string, error) {
	return t.table.Get(key)
}

func (t *LocalTable) Size() int {
	return t.table.Size()
}

func (t *LocalTable) Iter() hash_ring.KeyValueIterator {
	return t.table.Iter()
}

func (t *LocalTable) Erase(key string) {
	t.table.Erase(key)
}
