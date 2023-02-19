package hash_ring

type PanicTable struct{}

func (t *PanicTable) Add(key string, value string) error {
	panic("Add operation should have never been called")
}

func (t *PanicTable) Get(key string) (*string, error) {
	panic("Add operation should have never been called")
}

func (t *PanicTable) Size() int {
	panic("Add operation should have never been called")
}

func (t *PanicTable) Iter() KeyValueIterator {
	panic("Add operation should have never been called")
}

func (t *PanicTable) Erase(key string) {
	panic("Add operation should have never been called")
}
