package hash_ring

import "sync"

type Value struct {
	value string
	meta  ValueMeta
}

type InMemoryTable struct {
	data map[string]Value
	lock sync.Mutex
}

func NewInMemoryTable() InMemoryTable { return InMemoryTable{make(map[string]Value), sync.Mutex{}} }

func (t *InMemoryTable) Add(key string, value string, meta *ValueMeta) error {
	defer t.lock.Unlock()
	t.lock.Lock()
	t.data[key] = Value{value: value, meta: *meta}
	return nil
}

func (t *InMemoryTable) Get(key string) (*string, *ValueMeta, error) {
	defer t.lock.Unlock()
	t.lock.Lock()
	value, success := t.data[key]
	if success {
		return &value.value, &value.meta, nil
	} else {
		return nil, nil, nil
	}
}

func (t *InMemoryTable) Size() int {
	defer t.lock.Unlock()
	t.lock.Lock()
	return len(t.data)
}

type iterator struct {
	current_index int
	keys          []string
	data          map[string]Value
}

func (t *iterator) Next() (*string, *string, *ValueMeta) {
	t.current_index++
	if t.current_index < len(t.data) {
		key := t.keys[t.current_index]
		value := t.data[key]
		return &key, &value.value, &value.meta
	} else {
		return nil, nil, nil
	}
}

func (t *InMemoryTable) Iter() KeyValueIterator {
	keys := make([]string, 0, len(t.data))
	for k := range t.data {
		keys = append(keys, k)
	}

	return &iterator{-1, keys, t.data}
}

func (t *InMemoryTable) Erase(key string) {
	defer t.lock.Unlock()
	t.lock.Lock()
	delete(t.data, key)
}
