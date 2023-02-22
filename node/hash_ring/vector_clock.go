package hash_ring

type VectorClock struct {
	Counts map[int]int
}

func (clock *VectorClock) get(i int) int {
	v, success := clock.Counts[i]
	if success {
		return v
	} else {
		return 0
	}
}

func NewVectorClock() VectorClock {
	return VectorClock{make(map[int]int)}
}

func MaxInt(x, y int) int {
	if x < y {
		return y
	}
	return x
}

func MaxUpVectorClock(clock1 VectorClock, clock2 VectorClock) VectorClock {
	new_clock := VectorClock{make(map[int]int)}
	for key := range clock1.Counts {
		new_clock.Counts[key] = MaxInt(clock1.get(key), clock1.get(key))
	}
	for key := range clock2.Counts {
		new_clock.Counts[key] = MaxInt(clock1.get(key), clock1.get(key))
	}
	return new_clock
}

func isNotCausal(left *VectorClock, right *VectorClock, keys map[int]int) bool {
	//any l > r
	for key := range keys {
		if left.get(key) > right.get(key) {
			return true
		}
	}
	//or
	//all l >= r
	//which can be any l < r, then return false
	for key := range keys {
		if left.get(key) < right.get(key) {
			return false
		}
	}
	return true
}

// !(left -> right)
func IsNotCausal(left *VectorClock, right *VectorClock) bool {
	return isNotCausal(left, right, left.Counts) && isNotCausal(left, right, left.Counts)
}
