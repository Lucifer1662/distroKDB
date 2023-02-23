package hash_ring

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVectorClockEqualsEmpty(t *testing.T) {
	v1 := NewVectorClock()
	v2 := NewVectorClock()

	assert.Equal(t, true, v1.Equals(v2))
}

func TestVectorClockEqualsTrueNotEmpty(t *testing.T) {
	v1 := NewVectorClock()
	v2 := NewVectorClock()

	v1.Add(0)
	v1.Add(0)
	v1.Add(1)

	v2.Add(0)
	v2.Add(0)
	v2.Add(1)

	assert.Equal(t, true, v1.Equals(v2))
}

func TestVectorClockEqualsFalseDifferentValue(t *testing.T) {
	v1 := NewVectorClock()
	v2 := NewVectorClock()

	v1.Add(0)
	v1.Add(0)
	v1.Add(1)

	v2.Add(0)
	v2.Add(0)
	v2.Add(1)
	v2.Add(1)

	assert.Equal(t, false, v1.Equals(v2))
}

func TestVectorClockEqualsFalseDifferentKeys(t *testing.T) {
	v1 := NewVectorClock()
	v2 := NewVectorClock()

	v1.Add(0)
	v1.Add(0)
	v1.Add(1)

	v2.Add(0)
	v2.Add(0)
	v2.Add(2)

	assert.Equal(t, false, v1.Equals(v2))
}

func TestFindLatestCasualVersionEmpty(t *testing.T) {
	latest_index := FindLatestCasualVersion([]*VectorClock{})
	assert.Equal(t, -1, latest_index)
}

func TestFindLatestCasualVersionSingleEntry(t *testing.T) {
	v1 := NewVectorClock()

	clocks := []*VectorClock{&v1}
	latest_index := FindLatestCasualVersion(clocks)
	assert.Equal(t, 0, latest_index)
	assert.Equal(t, &v1, clocks[0])
}

func TestFindLatestCasualMultipleVersionSame(t *testing.T) {
	v1 := NewVectorClock()

	clocks := []*VectorClock{&v1, &v1, &v1}
	latest_index := FindLatestCasualVersion(clocks)
	assert.Equal(t, 0, latest_index)
	assert.Equal(t, &v1, clocks[0])
	assert.Equal(t, &v1, clocks[1])
	assert.Equal(t, &v1, clocks[2])
}

func TestFindLatestCasualMultipleVersionSomeBehind(t *testing.T) {
	v1 := NewVectorClock()
	v2 := NewVectorClock()
	v3 := NewVectorClock()

	v1.Add(1)
	v1.Add(1)
	v1.Add(1)

	v1.Add(0)
	v1.Add(0)

	v2.Add(1)
	v2.Add(0)
	v2.Add(0)

	v3.Add(1)

	clocks := []*VectorClock{&v2, &v1, &v3, &v1, &v1}
	latest_index := FindLatestCasualVersion(clocks)

	var clock_nil *VectorClock = nil
	assert.Equal(t, 1, latest_index)
	assert.Equal(t, clock_nil, clocks[0])
	assert.Equal(t, &v1, clocks[1])
	assert.Equal(t, clock_nil, clocks[2])
	assert.Equal(t, &v1, clocks[3])
	assert.Equal(t, &v1, clocks[4])
}

func TestFindLatestCasualMultipleVersionConflicting(t *testing.T) {
	v1 := NewVectorClock()
	v2 := NewVectorClock()
	v3 := NewVectorClock()

	v1.Add(1)
	v1.Add(1)
	v1.Add(1)

	v1.Add(0)
	v1.Add(0)

	v2.Add(1)
	v2.Add(0)
	v2.Add(0)
	v2.Add(0)
	v2.Add(0)

	v3.Add(1)

	clocks := []*VectorClock{&v2, &v1, &v3, &v1, &v1}
	latest_index := FindLatestCasualVersion(clocks)

	var clock_nil *VectorClock = nil
	assert.Equal(t, -1, latest_index)
	assert.Equal(t, clock_nil, clocks[0])
	assert.Equal(t, clock_nil, clocks[1])
	assert.Equal(t, clock_nil, clocks[2])
	assert.Equal(t, clock_nil, clocks[3])
	assert.Equal(t, clock_nil, clocks[4])
}
