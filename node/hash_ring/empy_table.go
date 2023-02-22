package hash_ring

type EmptyTable struct{}

func (t *EmptyTable) Add(key string, value string, meta *ValueMeta) error {
	panic("Add operation should have never been called")
}

func (t *EmptyTable) Get(key string) (*string, *ValueMeta, error) {
	return nil, &ValueMeta{VectorClock: NewVectorClock()}, nil
}

func (t *EmptyTable) Size() int {
	return 0
}

func (t *EmptyTable) Iter() KeyValueIterator {
	panic("Add operation should have never been called")
}

func (t *EmptyTable) Erase(key string) {
	panic("Add operation should have never been called")
}
