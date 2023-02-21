package hash_ring

import "errors"

type ErrorTable struct{}

func (t *ErrorTable) Add(key string, value string, meta *ValueMeta) error {
	return errors.New("Failed")
}

func (t *ErrorTable) Get(key string) (*string, *ValueMeta, error) {
	return nil, nil, errors.New("Failed")
}

func (t *ErrorTable) Size() int {
	return 0
}

type ErrorIterator struct{}

func (t *ErrorIterator) Next() (*string, *string, *ValueMeta) {
	return nil, nil, nil
}

func (t *ErrorTable) Iter() KeyValueIterator {
	return &ErrorIterator{}
}

func (t *ErrorTable) Erase(key string) {
}
