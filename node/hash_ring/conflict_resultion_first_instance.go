package hash_ring

type ConflictResolutionFirstInstance struct{}

func (conflict *ConflictResolutionFirstInstance) Resolve(key string, values []*string, meta []*ValueMeta, nodes_position []uint64) *string {
	return values[0]
}
