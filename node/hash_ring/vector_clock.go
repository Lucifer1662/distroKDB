package hash_ring

type VectorClock struct {
	Counts map[int]int
}

func (clock *VectorClock) Copy() VectorClock {
	new_clock := VectorClock{make(map[int]int)}
	for key := range clock.Counts {
		new_clock.Counts[key] = clock.Counts[key]
	}
	return new_clock
}

func (clock *VectorClock) Get(i int) int {
	v, success := clock.Counts[i]
	if success {
		return v
	} else {
		return 0
	}
}

func (clock *VectorClock) Add(i int) {
	clock.Counts[i] = clock.Get(i) + 1
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
		new_clock.Counts[key] = MaxInt(clock1.Get(key), clock2.Get(key))
	}
	for key := range clock2.Counts {
		new_clock.Counts[key] = MaxInt(clock1.Get(key), clock2.Get(key))
	}
	return new_clock
}

func (clock *VectorClock) Equals(other_clock VectorClock) bool {
	keys := make(map[int]bool)
	for key := range clock.Counts {
		keys[key] = true
	}

	for key := range other_clock.Counts {
		keys[key] = true
	}

	for key := range keys {
		if clock.Get(key) != other_clock.Get(key) {
			return false
		}
	}

	return true
}

func MaxUpVectorClocks(clocks []VectorClock) VectorClock {
	keys := make(map[int]bool)
	for i := range clocks {
		for key := range clocks[i].Counts {
			keys[key] = true
		}
	}

	new_clock := VectorClock{make(map[int]int)}
	for key := range keys {
		max_count := 0
		for i := range clocks {
			if max_count < clocks[i].Get(key) {
				max_count = clocks[i].Get(key)
			}
		}
		new_clock.Counts[key] = max_count
	}

	return new_clock
}

func isNotCausal(left *VectorClock, right *VectorClock, keys map[int]bool) bool {
	//any l > r
	for key := range keys {
		if left.Get(key) > right.Get(key) {
			return true
		}
	}
	//or
	//all l >= r
	//which can be any l < r, then return false
	for key := range keys {
		if left.Get(key) < right.Get(key) {
			return false
		}
	}
	return true
}

// !(left -> right)
func IsNotCausal(left *VectorClock, right *VectorClock) bool {
	keys := make(map[int]bool)
	for key := range left.Counts {
		keys[key] = true
	}
	for key := range right.Counts {
		keys[key] = true
	}

	return isNotCausal(left, right, keys)
}

// !(left -> right) or !(right -> left)
// (left -> right) && (right -> left)
// aka a contradiction
func NeitherCausal(left *VectorClock, right *VectorClock) bool {
	return IsNotCausal(left, right) || IsNotCausal(right, left)
}

func AnyContradictionRelationships(clocks []VectorClock) bool {
	for i := range clocks {
		for j := 0; j < i; j++ {
			if i != j {
				if NeitherCausal(&clocks[i], &clocks[j]) {
					return true
				}
			}
		}
	}
	return false
}

func FindLatestCasualVersion(clocks []*VectorClock) int {
	best_counts := make(map[int]int)
	for i := range clocks {
		for key, count := range clocks[i].Counts {
			best_count, exists := best_counts[key]
			if !exists || count > best_count {
				best_counts[key] = count
			}
		}
	}

	for i := range clocks {
		for key := range best_counts {
			best_count := best_counts[key]
			my_count := clocks[i].Get(key)
			if my_count < best_count {
				clocks[i] = nil
				break
			}

		}
	}

	j := -1
	for i := range clocks {
		if clocks[i] != nil {
			if j == -1 {
				j = i
			} else {
				if !clocks[j].Equals(*clocks[i]) {
					return -1
				}
			}
		}
	}

	return j
}
