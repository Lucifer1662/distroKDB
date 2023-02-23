package hash_ring

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func SimpleHashRingDefaultTest(t *testing.T, hr *Hash_Ring) {
	var nil_string *string = nil

	assert.Greater(t, uint64(18446744073709551615)/2, Hash("bar"))
	assert.Less(t, uint64(18446744073709551615)/2, Hash("foo"))

	value_meta := ValueMeta{VectorClock: NewVectorClock()}

	hr.Add("bar", "bar", &value_meta)
	hr.Add("foo", "mar", &value_meta)

	val, _, _ := hr.Get("bar")
	assert.Equal(t, "bar", *val)

	val, _, _ = hr.Get("foo")
	assert.Equal(t, "mar", *val)

	val, _, _ = hr.Get("far")
	assert.Equal(t, nil_string, val)
}

func TestAddGetSomeData(t *testing.T) {
	hr := Hash_Ring{Generate_Nodes(5), 1, 1, 1, &ConflictResolutionFirstInstance{}, 0}
	for i := range hr.nodes {
		table := NewInMemoryTable()
		hr.nodes[i].table = &table
		tempTable := NewInMemoryTable()
		hr.nodes[i].temporaryTable = &tempTable
	}

	SimpleHashRingDefaultTest(t, &hr)
}

func TestCorrectNodeBoundsGenerated(t *testing.T) {
	nodes := Generate_Nodes(4)
	var partition_size uint64 = uint64(18446744073709551615) / 4
	assert.Equal(t, partition_size, nodes[0].position)
	assert.Equal(t, partition_size*2, nodes[1].position)
	assert.Equal(t, partition_size*3, nodes[2].position)
	assert.Equal(t, uint64(18446744073709551615), nodes[3].position)

	assert.Equal(t, uint64(0), nodes[0].physical_id)
	assert.Equal(t, uint64(1), nodes[1].physical_id)
	assert.Equal(t, uint64(2), nodes[2].physical_id)
	assert.Equal(t, uint64(3), nodes[3].physical_id)
}

func TestCorrectVirtualNodeBoundsGenerated(t *testing.T) {
	nodes := Generate_Nodes_With_Virtual(3, []int{2, 1, 2})
	var partition_size uint64 = uint64(18446744073709551615) / 5
	assert.Equal(t, partition_size, nodes[0].position)
	assert.Equal(t, partition_size*2, nodes[1].position)
	assert.Equal(t, partition_size*3, nodes[2].position)
	assert.Equal(t, partition_size*4, nodes[3].position)
	assert.Equal(t, uint64(18446744073709551615), nodes[4].position)

	assert.Equal(t, uint64(0), nodes[0].physical_id)
	assert.Equal(t, uint64(0), nodes[1].physical_id)
	assert.Equal(t, uint64(1), nodes[2].physical_id)
	assert.Equal(t, uint64(2), nodes[3].physical_id)
	assert.Equal(t, uint64(2), nodes[4].physical_id)
}

func TestDataSpreadsOut(t *testing.T) {
	hr := Hash_Ring{Generate_Nodes(5), 1, 1, 1, &ConflictResolutionFirstInstance{}, 0}
	for i := range hr.nodes {
		table := NewInMemoryTable()
		hr.nodes[i].table = &table
		tempTable := NewInMemoryTable()
		hr.nodes[i].temporaryTable = &tempTable
	}

	value_meta := ValueMeta{VectorClock: NewVectorClock()}

	for i := 0; i < 10000; i++ {
		hr.Add(fmt.Sprintf("%f{.9}", math.Cos(float64(i))), strconv.Itoa(i), &value_meta)
	}

	for _, node := range hr.nodes {
		assert.Greater(t, node.table.Size(), 1000)
		println(node.table.Size())
	}
}

func ReplicatedNumber(t *testing.T, nodes []Node, key string) int {
	count := 0
	for i := range nodes {
		val, _, err := nodes[i].Get(key)
		if err == nil && val != nil {
			print(i)
			print(",")
			count++
		}
	}
	println()
	return count
}

func ReplicatedMatchesIndexes(t *testing.T, nodes []Node, key string) []int {
	nums := []int{}
	for i := range nodes {
		val, _, err := nodes[i].Get(key)
		if err == nil && val != nil {
			nums = append(nums, i)
		}
	}
	return nums
}

func ReplicatedMatchesIds(t *testing.T, nodes []Node, key string) []int {
	nums := []int{}
	for i := range nodes {
		val, _, err := nodes[i].Get(key)
		if err == nil && val != nil {
			nums = append(nums, int(nodes[i].physical_id))
		}
	}
	return nums
}

func TestReplicateAllSuccessfully(t *testing.T) {
	hr := Hash_Ring{Generate_Nodes(5), 3, 3, 3, &ConflictResolutionFirstInstance{}, 0}

	for i := range hr.nodes {
		table := NewInMemoryTable()
		hr.nodes[i].table = &table
		tempTable := NewInMemoryTable()
		hr.nodes[i].temporaryTable = &tempTable
	}

	SimpleHashRingDefaultTest(t, &hr)

	assert.Equal(t, []int{0, 1, 2}, ReplicatedMatchesIndexes(t, hr.nodes, "bar"))
	assert.Equal(t, []int{0, 1, 4}, ReplicatedMatchesIndexes(t, hr.nodes, "foo"))
}

func TestReplicateAllSuccessfullyVirtual(t *testing.T) {
	hr := Hash_Ring{Generate_Nodes_With_Virtual(5, []int{2, 2, 2, 2, 2}), 3, 3, 3, &ConflictResolutionFirstInstance{}, 0}

	for i := range hr.nodes {
		table := NewInMemoryTable()
		hr.nodes[i].table = &table
		tempTable := NewInMemoryTable()
		hr.nodes[i].temporaryTable = &tempTable
	}

	SimpleHashRingDefaultTest(t, &hr)

	assert.Equal(t, []int{0, 2, 4}, ReplicatedMatchesIndexes(t, hr.nodes, "bar"))
	assert.Equal(t, []int{0, 1, 2}, ReplicatedMatchesIds(t, hr.nodes, "bar"))

	assert.Equal(t, []int{0, 2, 8}, ReplicatedMatchesIndexes(t, hr.nodes, "foo"))
	assert.Equal(t, []int{0, 1, 4}, ReplicatedMatchesIds(t, hr.nodes, "foo"))
}

type DelayAddTable struct {
	table     KeyValueTable
	wait_chan chan bool
}

func (t *DelayAddTable) Add(key string, value string, meta *ValueMeta) error {
	<-t.wait_chan
	result := t.table.Add(key, value, meta)
	t.wait_chan <- true
	return result
}

func (t *DelayAddTable) Get(key string) (*string, *ValueMeta, error) {
	return t.table.Get(key)
}

func (t *DelayAddTable) Size() int {
	return t.table.Size()
}

func (t *DelayAddTable) Iter() KeyValueIterator {
	panic("Should not be called")
}

func (t *DelayAddTable) Erase(key string) {
	panic("Should not be called")
}

func TestReplicateAllSuccessfullySlowNode(t *testing.T) {
	hr := Hash_Ring{Generate_Nodes(5), 3, 2, 2, &ConflictResolutionFirstInstance{}, 0}

	for i := range hr.nodes {
		table := NewInMemoryTable()
		hr.nodes[i].table = &table
		tempTable := NewInMemoryTable()
		hr.nodes[i].temporaryTable = &tempTable
	}
	slow_table := DelayAddTable{hr.nodes[0].table, make(chan bool)}
	hr.nodes[0].table = &slow_table

	value_meta := ValueMeta{VectorClock: NewVectorClock()}

	hr.Add("bar", "mar", &value_meta)

	assert.Equal(t, []int{1, 2}, ReplicatedMatchesIndexes(t, hr.nodes, "bar"))
	slow_table.wait_chan <- true //send update
	<-slow_table.wait_chan       //wait for add
	assert.Equal(t, []int{0, 1, 2}, ReplicatedMatchesIndexes(t, hr.nodes, "bar"))
}

func TestReplicateSinglePartialFailure(t *testing.T) {
	hr := Hash_Ring{Generate_Nodes(5), 3, 3, 3, &ConflictResolutionFirstInstance{}, 0}

	for i := range hr.nodes {
		table := NewInMemoryTable()
		hr.nodes[i].table = &table
		tempTable := NewInMemoryTable()
		hr.nodes[i].temporaryTable = &tempTable
	}
	error_table := ErrorTable{}
	hr.nodes[0].table = &error_table

	value_meta := ValueMeta{VectorClock: NewVectorClock()}

	hr.Add("bar", "mar", &value_meta)

	assert.Equal(t, []int{1, 2, 3}, ReplicatedMatchesIndexes(t, hr.nodes, "bar"))
}

func TestReplicateSinglePartialFailureVirtual(t *testing.T) {
	hr := Hash_Ring{Generate_Nodes_With_Virtual(5, []int{2, 2, 2, 2, 2}), 3, 3, 3, &ConflictResolutionFirstInstance{}, 0}

	for i := range hr.nodes {
		table := NewInMemoryTable()
		hr.nodes[i].table = &table
		tempTable := NewInMemoryTable()
		hr.nodes[i].temporaryTable = &tempTable
	}
	error_table := ErrorTable{}
	hr.nodes[0].table = &error_table

	value_meta := ValueMeta{VectorClock: NewVectorClock()}

	hr.Add("bar", "mar", &value_meta)
	//normally "bar" is on 0,1,2, but since 0 fails it will be on 1,2,3
	assert.Equal(t, []int{1, 2, 3}, ReplicatedMatchesIds(t, hr.nodes, "bar"))
	assert.Equal(t, []int{2, 4, 6}, ReplicatedMatchesIndexes(t, hr.nodes, "bar"))
}

func TestReplicateMultiplePartialFailure(t *testing.T) {
	hr := Hash_Ring{Generate_Nodes(5), 3, 3, 3, &ConflictResolutionFirstInstance{}, 0}

	for i := range hr.nodes {
		table := NewInMemoryTable()
		hr.nodes[i].table = &table
		tempTable := NewInMemoryTable()
		hr.nodes[i].temporaryTable = &tempTable
	}
	error_table := ErrorTable{}

	hr.nodes[0].table = &error_table
	hr.nodes[1].table = &error_table

	value_meta := ValueMeta{VectorClock: NewVectorClock()}

	hr.Add("bar", "mar", &value_meta)

	assert.Equal(t, []int{2, 3, 4}, ReplicatedMatchesIndexes(t, hr.nodes, "bar"))
}

func TestReplicateFullFailureSomeCommit(t *testing.T) {
	hr := Hash_Ring{Generate_Nodes(5), 3, 3, 3, &ConflictResolutionFirstInstance{}, 0}

	error_table := ErrorTable{}

	memory_table1 := NewInMemoryTable()
	memory_table2 := NewInMemoryTable()
	hr.nodes[0].table = &error_table
	hr.nodes[1].table = &memory_table1
	hr.nodes[2].table = &memory_table2
	hr.nodes[3].temporaryTable = &error_table
	hr.nodes[4].temporaryTable = &error_table

	value_meta := ValueMeta{VectorClock: NewVectorClock()}

	err := hr.Add("bar", "mar", &value_meta)

	assert.NotNil(t, err)

	val, _, _ := memory_table1.Get("bar")
	assert.Equal(t, "mar", *val)

	val, _, _ = memory_table2.Get("bar")
	assert.Equal(t, "mar", *val)
}

func TestReplicateFullFailure(t *testing.T) {
	hr := Hash_Ring{Generate_Nodes(5), 3, 3, 3, &ConflictResolutionFirstInstance{}, 0}

	error_table := ErrorTable{}
	for i := range hr.nodes {
		hr.nodes[i].temporaryTable = &error_table
		hr.nodes[i].table = &error_table
	}

	value_meta := ValueMeta{VectorClock: NewVectorClock()}

	err := hr.Add("bar", "mar", &value_meta)
	assert.NotNil(t, err)
}

type SavePositionConflictResolution struct {
	Nodes_positions []uint64
	Values          []string
	Was_Called      bool
}

func (conflict *SavePositionConflictResolution) Resolve(key string, values []*string, metas []*ValueMeta, nodes_position []uint64) *string {
	sort.SliceStable(nodes_position, func(i, j int) bool {
		return nodes_position[i] < nodes_position[j]
	})
	conflict.Nodes_positions = nodes_position
	conflict.Values = make([]string, len(values))
	for i := range values {
		if values[i] == nil {
			conflict.Values[i] = ""
		} else {
			conflict.Values[i] = *values[i]
		}
	}
	conflict.Was_Called = true
	sort.SliceStable(values, func(i, j int) bool {
		return *values[i] < *values[j]
	})
	return values[0]
}

func TestRetrieveAllSuccessfully(t *testing.T) {
	resolution := &SavePositionConflictResolution{[]uint64{}, []string{}, false}
	hr := Hash_Ring{Generate_Nodes(5), 3, 3, 3, resolution, 0}

	for i := range hr.nodes {
		table := NewInMemoryTable()
		hr.nodes[i].table = &table
		tempTable := NewInMemoryTable()
		hr.nodes[i].temporaryTable = &tempTable
	}

	value_meta := ValueMeta{VectorClock: NewVectorClock()}

	hr.nodes[0].Add("bar", "mar", &value_meta)
	hr.nodes[1].Add("bar", "mar", &value_meta)
	hr.nodes[2].Add("bar", "mar", &value_meta)

	hr.Get("bar")

	var partition_size uint64 = uint64(18446744073709551615) / 5
	assert.Equal(t, []uint64{partition_size * 1, partition_size * 2, partition_size * 3}, resolution.Nodes_positions)
	assert.Equal(t, []string{"mar", "mar", "mar"}, resolution.Values)
}

func TestRetrievePartialSuccessfully(t *testing.T) {
	resolution := &SavePositionConflictResolution{[]uint64{}, []string{}, false}
	hr := Hash_Ring{Generate_Nodes(5), 3, 3, 3, resolution, 0}

	for i := range hr.nodes {
		table := NewInMemoryTable()
		hr.nodes[i].table = &table
		tempTable := NewInMemoryTable()
		hr.nodes[i].temporaryTable = &tempTable
	}

	error_table := ErrorTable{}
	hr.nodes[2].table = &error_table

	value_meta := ValueMeta{VectorClock: NewVectorClock()}

	hr.nodes[0].AddPermanent("bar", "mar", &value_meta)
	hr.nodes[1].AddPermanent("bar", "mar", &value_meta)
	hr.nodes[3].AddTemporary("bar", "mar", &value_meta)

	hr.Get("bar")

	var partition_size uint64 = uint64(18446744073709551615) / 5
	assert.Equal(t, []uint64{partition_size * 1, partition_size * 2, partition_size * 4}, resolution.Nodes_positions)
	assert.Equal(t, []string{"mar", "mar", "mar"}, resolution.Values)
}

func TestRetrievePartialFailure(t *testing.T) {
	resolution := &SavePositionConflictResolution{[]uint64{}, []string{}, false}
	hr := Hash_Ring{Generate_Nodes(5), 3, 3, 3, resolution, 0}

	error_table := ErrorTable{}
	for i := range hr.nodes {
		hr.nodes[i].temporaryTable = &error_table
	}

	memory_table := NewInMemoryTable()
	hr.nodes[0].table = &error_table
	hr.nodes[1].table = &memory_table
	hr.nodes[2].table = &memory_table
	hr.nodes[3].table = &error_table
	hr.nodes[4].table = &error_table

	hr.Get("bar")

	assert.Equal(t, false, resolution.Was_Called)
}

func TestRetrieveFullFailure(t *testing.T) {
	resolution := &SavePositionConflictResolution{[]uint64{}, []string{}, false}
	hr := Hash_Ring{Generate_Nodes(5), 3, 3, 3, resolution, 0}

	for i := range hr.nodes {
		tempTable := NewInMemoryTable()
		hr.nodes[i].temporaryTable = &tempTable
	}

	error_table := ErrorTable{}
	hr.nodes[0].table = &error_table
	hr.nodes[1].table = &error_table
	hr.nodes[2].table = &error_table
	hr.nodes[3].table = &error_table
	hr.nodes[4].table = &error_table

	hr.Get("bar")

	assert.Equal(t, false, resolution.Was_Called)
}

func TestReplicateToPrimaryFull(t *testing.T) {
	resolution := &SavePositionConflictResolution{[]uint64{}, []string{}, false}
	hr := Hash_Ring{Generate_Nodes(5), 3, 3, 3, resolution, 0}

	for i := range hr.nodes {
		table := NewInMemoryTable()
		hr.nodes[i].table = &table
		tempTable := NewInMemoryTable()
		hr.nodes[i].temporaryTable = &tempTable
	}

	value_meta := ValueMeta{VectorClock: NewVectorClock()}

	replicated_to := hr.ReplicateToPrimary("foo", "mar", &value_meta)

	assert.Equal(t, 3, replicated_to)

	hr.Get("foo")

	var partition_size uint64 = uint64(18446744073709551615) / 5
	assert.Equal(t, []uint64{partition_size * 1, partition_size * 2, partition_size * 5}, resolution.Nodes_positions)
	assert.Equal(t, []string{"mar", "mar", "mar"}, resolution.Values)
}

func TestReplicateToPrimaryPartial(t *testing.T) {
	resolution := &SavePositionConflictResolution{[]uint64{}, []string{}, false}
	hr := Hash_Ring{Generate_Nodes(5), 3, 3, 3, resolution, 0}

	for i := range hr.nodes {
		table := NewInMemoryTable()
		hr.nodes[i].table = &table
		tempTable := NewInMemoryTable()
		hr.nodes[i].temporaryTable = &tempTable
	}

	error_table := ErrorTable{}
	hr.nodes[1].table = &error_table

	value_meta := ValueMeta{VectorClock: NewVectorClock()}

	replicated_to := hr.ReplicateToPrimary("foo", "mar", &value_meta)

	assert.Equal(t, 2, replicated_to)

	hr.Get("foo")

	var partition_size uint64 = uint64(18446744073709551615) / 5
	assert.Equal(t, []uint64{partition_size * 1, partition_size * 5}, resolution.Nodes_positions)
	assert.Equal(t, []string{"mar", "mar"}, resolution.Values)
}

func TestRetrievePartialSuccessfullyRecovery(t *testing.T) {
	resolution := &SavePositionConflictResolution{[]uint64{}, []string{}, false}
	hr := Hash_Ring{Generate_Nodes(5), 3, 3, 3, resolution, 0}

	tempTable := NewInMemoryTable()
	for i := range hr.nodes {
		table := NewInMemoryTable()
		hr.nodes[i].table = &table
		hr.nodes[i].temporaryTable = &tempTable
	}

	value_meta := ValueMeta{VectorClock: NewVectorClock()}

	hr.nodes[0].AddPermanent("bar", "mar", &value_meta)
	hr.nodes[1].AddPermanent("bar", "mar", &value_meta)
	hr.nodes[3].AddTemporary("bar", "mar", &value_meta)

	Cleanup_temporary(&hr, &tempTable)

	hr.Get("bar")

	var partition_size uint64 = uint64(18446744073709551615) / 5
	assert.Equal(t, []uint64{partition_size * 1, partition_size * 2, partition_size * 3}, resolution.Nodes_positions)
	assert.Equal(t, []string{"mar", "mar", "mar"}, resolution.Values)

	val, _, err := hr.nodes[3].Get("bar")
	assert.Nil(t, err)
	var nil_string *string = nil
	assert.Equal(t, nil_string, val)
}

func TestRetrievePartialUnsuccessfullyRecovery(t *testing.T) {
	resolution := &SavePositionConflictResolution{[]uint64{}, []string{}, false}
	hr := Hash_Ring{Generate_Nodes(5), 3, 3, 3, resolution, 0}

	tempTable := NewInMemoryTable()
	for i := range hr.nodes {
		table := NewInMemoryTable()
		hr.nodes[i].table = &table
		hr.nodes[i].temporaryTable = &tempTable
	}

	//2 will remain to error all request
	error_table := ErrorTable{}
	hr.nodes[2].table = &error_table

	value_meta := ValueMeta{VectorClock: NewVectorClock()}

	hr.nodes[0].AddPermanent("bar", "mar", &value_meta)
	hr.nodes[1].AddPermanent("bar", "mar", &value_meta)
	hr.nodes[3].AddTemporary("bar", "mar", &value_meta)

	var partition_size uint64 = uint64(18446744073709551615) / 5

	//previous start
	hr.Get("bar")
	assert.Equal(t, []uint64{partition_size * 1, partition_size * 2, partition_size * 4}, resolution.Nodes_positions)
	assert.Equal(t, []string{"mar", "mar", "mar"}, resolution.Values)

	Cleanup_temporary(&hr, &tempTable)

	//same state remains
	hr.Get("bar")
	assert.Equal(t, []uint64{partition_size * 1, partition_size * 2, partition_size * 4}, resolution.Nodes_positions)
	assert.Equal(t, []string{"mar", "mar", "mar"}, resolution.Values)

	val, _, err := hr.nodes[3].Get("bar")
	assert.Nil(t, err)
	assert.Equal(t, "mar", *val)
}
