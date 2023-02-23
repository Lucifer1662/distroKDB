package hash_ring

import "sync"

type ProxyTable struct {
	table        KeyValueTable
	hr           *Hash_Ring
	key_position KeyHash
	lock         sync.Mutex
	isPermanent  bool
}

func (t *ProxyTable) Add(key string, value string, meta *ValueMeta) error {
	defer t.lock.Unlock()
	t.lock.Lock()
	if t.isPermanent {
		return t.hr.AddToNodePermanent(t.key_position, key, value, meta)
	} else {
		return t.hr.AddToNodeTemporary(t.key_position, key, value, meta)
	}
}

func (t *ProxyTable) Get(key string) (*string, *ValueMeta, error) {
	defer t.lock.Unlock()
	t.lock.Lock()
	if t.isPermanent {
		return t.hr.GetFromNodePermanent(t.key_position, key)
	} else {
		return t.hr.GetFromNodeTemporary(t.key_position, key)
	}
}

func (t *ProxyTable) Size() int {
	defer t.lock.Unlock()
	t.lock.Lock()
	panic("Unimplemented")
}

func (t *ProxyTable) Iter() KeyValueIterator {
	panic("Unimplemented")
}

func (t *ProxyTable) Erase(key string) {
	panic("Unimplemented")
}
