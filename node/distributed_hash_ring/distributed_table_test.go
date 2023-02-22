package distributed_hash_ring

import (
	"testing"

	"github.com/lucifer1662/distrokdb/node/hash_ring"

	"github.com/stretchr/testify/assert"
)

func TestDistributedTableAdd(t *testing.T) {
	nodes1 := hash_ring.Generate_Nodes(2)
	hr1 := hash_ring.New(nodes1, 1, 1, 1, &hash_ring.ConflictResolutionFirstInstance{})
	nodes1[0].SetTable(&DistributedTable{"localhost:1234", nodes1[0].GetPosition()})
	nodes1[0].SetTemporaryTable(&hash_ring.EmptyTable{})
	table1 := hash_ring.NewInMemoryTable()
	nodes1[1].SetTable(&LocalTable{&table1})
	nodes1[1].SetTemporaryTable(&hash_ring.EmptyTable{})

	nodes2 := hash_ring.Generate_Nodes(2)
	hr2 := hash_ring.New(nodes2, 1, 1, 1, &hash_ring.ConflictResolutionFirstInstance{})
	table2 := hash_ring.NewInMemoryTable()
	nodes2[0].SetTable(&LocalTable{&table2})
	nodes2[0].SetTemporaryTable(&hash_ring.EmptyTable{})
	nodes2[1].SetTable(&DistributedTable{"localhost:1235", nodes2[1].GetPosition()})
	nodes2[1].SetTemporaryTable(&hash_ring.EmptyTable{})

	server1 := NewServer(&hr1, 1235)
	server2 := NewServer(&hr2, 1234)

	server1.Start()
	server2.Start()

	//make sure "bar" corresponds to node 0, and foo to node 1
	assert.Greater(t, uint64(18446744073709551615)/2, hash_ring.Hash("bar"))
	assert.Less(t, uint64(18446744073709551615)/2, hash_ring.Hash("foo"))

	value_meta := hash_ring.NewValueMeta(hash_ring.NewVectorClock())

	hr1.Add("bar", "bar", value_meta)
	val, err := hr1.Get("bar")
	assert.Equal(t, "bar", *val)
	assert.Equal(t, nil, err)

	val, err = hr2.Get("bar")
	assert.Equal(t, "bar", *val)
	assert.Equal(t, nil, err)

	hr2.Add("foo", "foo", value_meta)
	val, err = hr2.Get("foo")
	assert.Equal(t, "foo", *val)
	assert.Equal(t, nil, err)

	val, err = hr1.Get("foo")
	assert.Equal(t, "foo", *val)
	assert.Equal(t, nil, err)
}

func TestDistributedTableAddWithConfig(t *testing.T) {
	positions := hash_ring.Generate_Ring_Positions(2)

	shared_config := SharedConfig{
		Nodes: []Node{
			{
				Position:    positions[0],
				Address:     "localhost:1234",
				Id:          0,
				Physical_Id: 0,
			},
			{
				Position:    positions[1],
				Address:     "localhost:1235",
				Id:          1,
				Physical_Id: 1,
			},
		},
		Replication_factor: 1,
		Minimum_writes:     1,
		Minimum_read:       1,
	}

	node1_config := InstanceConfig{&shared_config, 0, 1234}
	node2_config := InstanceConfig{&shared_config, 1, 1235}

	hr1 := New(&node1_config)
	hr2 := New(&node2_config)

	server1 := NewServer(&hr1, 1234)
	server2 := NewServer(&hr2, 1235)

	server1.Start()
	server2.Start()

	//make sure "bar" corresponds to node 0, and foo to node 1
	assert.Greater(t, positions[0], hash_ring.Hash("bar"))
	assert.Less(t, positions[1], hash_ring.Hash("foo"))

	value_meta := hash_ring.NewValueMeta(hash_ring.NewVectorClock())

	hr1.Add("bar", "bar", value_meta)
	val, err := hr1.Get("bar")
	assert.Equal(t, "bar", *val)
	assert.Equal(t, nil, err)

	val, err = hr2.Get("bar")
	assert.Equal(t, "bar", *val)
	assert.Equal(t, nil, err)

	hr2.Add("foo", "foo", value_meta)
	val, err = hr2.Get("foo")
	assert.Equal(t, "foo", *val)
	assert.Equal(t, nil, err)

	val, err = hr1.Get("foo")
	assert.Equal(t, "foo", *val)
	assert.Equal(t, nil, err)
}
