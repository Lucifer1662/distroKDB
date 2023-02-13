package hash_ring

import "sync"

type InMemoryTable struct {
	data map[string]string
	lock sync.Mutex
}

func NewInMemoryTable() InMemoryTable { return InMemoryTable{make(map[string]string), sync.Mutex{}} }

func (t *InMemoryTable) Add(key string, value string) error {
	defer t.lock.Unlock()
	t.lock.Lock()
	t.data[key] = value
	return nil
}

func (t *InMemoryTable) Get(key string) (*string, error) {
	defer t.lock.Unlock()
	t.lock.Lock()
	value, success := t.data[key]
	if success {
		return &value, nil
	} else {
		return nil, nil
	}
}

func (t *InMemoryTable) Remove(key string) error {
	defer t.lock.Unlock()
	t.lock.Lock()
	delete(t.data, key)
	return nil
}

func (t *InMemoryTable) Size() int {
	defer t.lock.Unlock()
	t.lock.Lock()
	return len(t.data)
}
