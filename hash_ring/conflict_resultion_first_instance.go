package hash_ring

type ConflictResolutionFirstInstance struct{}

func (conflict *ConflictResolutionFirstInstance) Resolve(key string, values []*string, nodes_position []uint64) int {
	return 0
}
