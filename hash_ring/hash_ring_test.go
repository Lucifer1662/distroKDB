package hash_ring

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func SimpleHashRingDefaultTest(t *testing.T, hr *Hash_Ring) {
	var nil_string *string = nil

	assert.Greater(t, uint64(18446744073709551615)/2, hash("bar"))
	assert.Less(t, uint64(18446744073709551615)/2, hash("foo"))

	hr.Add("bar", "bar")
	hr.Add("foo", "mar")

	val, _ := hr.Get("bar")
	assert.Equal(t, "bar", *val)

	val, _ = hr.Get("foo")
	assert.Equal(t, "mar", *val)

	val, _ = hr.Get("far")
	assert.Equal(t, nil_string, val)
}

func TestAddGetSomeData(t *testing.T) {
	hr := Hash_Ring{Generate_Nodes(5), 1, 1, 1}
	for i := range hr.nodes {
		table := NewInMemoryTable()
		hr.nodes[i].table = &table
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

}

func TestDataSpreadsOut(t *testing.T) {
	hr := Hash_Ring{Generate_Nodes(5), 1, 1, 1}
	for i := range hr.nodes {
		table := NewInMemoryTable()
		hr.nodes[i].table = &table
	}

	for i := 0; i < 10000; i++ {
		hr.Add(fmt.Sprintf("%f{.9}", math.Cos(float64(i))), strconv.Itoa(i))
	}

	for _, node := range hr.nodes {
		assert.Greater(t, node.table.Size(), 1000)
		println(node.table.Size())
	}
}

func ReplicatedNumber(t *testing.T, nodes []Node, key string) int {
	count := 0
	for i := range nodes {
		val, err := nodes[i].Get(key)
		if err == nil && val != nil {
			print(i)
			print(",")
			count++
		}
	}
	println()
	return count
}

func ReplicatedMatches(t *testing.T, nodes []Node, key string) []int {
	nums := []int{}
	for i := range nodes {
		val, err := nodes[i].Get(key)
		if err == nil && val != nil {
			nums = append(nums, i)
		}
	}
	return nums
}

func TestReplicateAllSuccessfully(t *testing.T) {
	hr := Hash_Ring{Generate_Nodes(5), 3, 3, 3}

	for i := range hr.nodes {
		table := NewInMemoryTable()
		hr.nodes[i].table = &table
	}

	SimpleHashRingDefaultTest(t, &hr)

	assert.Equal(t, []int{0, 1, 2}, ReplicatedMatches(t, hr.nodes, "bar"))
	assert.Equal(t, []int{0, 1, 4}, ReplicatedMatches(t, hr.nodes, "foo"))
}

type DelayAddTable struct {
	table     KeyValueTable
	wait_chan chan bool
}

func (t *DelayAddTable) Add(key string, value string) error {
	<-t.wait_chan
	result := t.table.Add(key, value)
	t.wait_chan <- true
	return result
}

func (t *DelayAddTable) Get(key string) (*string, error) {
	return t.table.Get(key)
}

func (t *DelayAddTable) Remove(key string) error {
	return t.table.Remove(key)
}

func (t *DelayAddTable) Size() int {
	return t.table.Size()
}

func TestReplicateAllSuccessfullySlowNode(t *testing.T) {
	hr := Hash_Ring{Generate_Nodes(5), 3, 2, 2}

	for i := range hr.nodes {
		table := NewInMemoryTable()
		hr.nodes[i].table = &table
	}
	slow_table := DelayAddTable{hr.nodes[0].table, make(chan bool)}
	hr.nodes[0].table = &slow_table

	hr.Add("bar", "mar")

	assert.Equal(t, []int{1, 2}, ReplicatedMatches(t, hr.nodes, "bar"))
	slow_table.wait_chan <- true //send update
	<-slow_table.wait_chan       //wait for add
	assert.Equal(t, []int{0, 1, 2}, ReplicatedMatches(t, hr.nodes, "bar"))
}

type ErrorTable struct{}

func (t *ErrorTable) Add(key string, value string) error {
	return errors.New("Failed")
}

func (t *ErrorTable) Get(key string) (*string, error) {
	return nil, errors.New("Failed")
}

func (t *ErrorTable) Remove(key string) error {
	return errors.New("Failed")
}

func (t *ErrorTable) Size() int {
	return 0
}
func TestReplicateSinglePartialFailure(t *testing.T) {
	hr := Hash_Ring{Generate_Nodes(5), 3, 3, 3}

	for i := range hr.nodes {
		table := NewInMemoryTable()
		hr.nodes[i].table = &table
	}
	error_table := ErrorTable{}
	hr.nodes[0].table = &error_table

	hr.Add("bar", "mar")

	assert.Equal(t, []int{1, 2, 3}, ReplicatedMatches(t, hr.nodes, "bar"))
}

func TestReplicateMultiplePartialFailure(t *testing.T) {
	hr := Hash_Ring{Generate_Nodes(5), 3, 3, 3}

	for i := range hr.nodes {
		table := NewInMemoryTable()
		hr.nodes[i].table = &table
	}
	error_table := ErrorTable{}
	hr.nodes[0].table = &error_table
	hr.nodes[1].table = &error_table

	hr.Add("bar", "mar")

	assert.Equal(t, []int{2, 3, 4}, ReplicatedMatches(t, hr.nodes, "bar"))
}

func TestReplicateFullFailureSomeCommit(t *testing.T) {
	hr := Hash_Ring{Generate_Nodes(5), 3, 3, 3}

	memory_table := NewInMemoryTable()
	error_table := ErrorTable{}
	hr.nodes[0].table = &error_table
	hr.nodes[1].table = &memory_table
	hr.nodes[2].table = &error_table
	hr.nodes[3].table = &error_table
	hr.nodes[4].table = &error_table

	err := hr.Add("bar", "mar")

	assert.NotNil(t, err)

	val, _ := memory_table.Get("bar")
	assert.Equal(t, "mar", *val)
}

func TestReplicateFullFailure(t *testing.T) {
	hr := Hash_Ring{Generate_Nodes(5), 3, 3, 3}

	error_table := ErrorTable{}
	hr.nodes[0].table = &error_table
	hr.nodes[1].table = &error_table
	hr.nodes[2].table = &error_table
	hr.nodes[3].table = &error_table
	hr.nodes[4].table = &error_table

	err := hr.Add("bar", "mar")
	assert.NotNil(t, err)
}
