package distributed_hash_ring

import "github.com/lucifer1662/distrokdb/node/hash_ring"

type LocalTable struct {
	table hash_ring.KeyValueTable
}

func (t *LocalTable) Add(key string, value string, meta *hash_ring.ValueMeta) error {
	return t.table.Add(key, value, meta)
}

func (t *LocalTable) Get(key string) (*string, *hash_ring.ValueMeta, error) {
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
