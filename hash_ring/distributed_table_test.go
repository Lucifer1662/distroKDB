package hash_ring

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDistributedTableAdd(t *testing.T) {

	hr1 := Hash_Ring{Generate_Nodes(2), 1, 1, 1}
	hr1.nodes[0].table = &DistributedTable{"localhost:1234"}
	table1 := NewInMemoryTable()
	hr1.nodes[1].table = &LocalTable{&table1}

	hr2 := Hash_Ring{Generate_Nodes(2), 1, 1, 1}
	table2 := NewInMemoryTable()
	hr2.nodes[0].table = &LocalTable{&table2}
	hr2.nodes[1].table = &DistributedTable{"localhost:1235"}

	server1 := DistributedHashRingServer{&hr1}
	server2 := DistributedHashRingServer{&hr2}

	server1.Start(1235)
	server2.Start(1234)

	//make sure "bar" corrosponds to node 0, and foo to node 1
	assert.Greater(t, uint64(18446744073709551615)/2, hash("bar"))
	assert.Less(t, uint64(18446744073709551615)/2, hash("foo"))

	hr1.Add("bar", "bar")
	val, err := hr1.Get("bar")
	assert.Equal(t, "bar", *val)
	assert.Equal(t, nil, err)

	val, err = hr2.Get("bar")
	assert.Equal(t, "bar", *val)
	assert.Equal(t, nil, err)

	hr2.Add("foo", "foo")
	val, err = hr2.Get("foo")
	assert.Equal(t, "foo", *val)
	assert.Equal(t, nil, err)

	val, err = hr1.Get("foo")
	assert.Equal(t, "foo", *val)
	assert.Equal(t, nil, err)
}
