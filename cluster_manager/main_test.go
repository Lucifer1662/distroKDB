package main

import (
	"testing"

	"github.com/lucifer1662/distrokdb/node/hash_ring"
	"github.com/stretchr/testify/assert"
)

func TestInit(t *testing.T) {
	var number_of_virtual_nodes int = 2
	var number_of_nodes int = 3
	var replication_factor int = 2
	var minimum_writes int = 2
	var minimum_read int = 2
	base_node := Node{}
	base_node.Address = "address"
	base_node.Http_node_address = "http address"
	base_node.Node_config_address = "config address"
	base_node.Http_port = 8080
	base_node.Internal_port = 6000

	manager := New(number_of_nodes, base_node, number_of_virtual_nodes, replication_factor, minimum_writes, minimum_read)

	ring_positions := hash_ring.Generate_Ring_Positions(6)

	base_node.Physical_Id = 0
	base_node.Id = 0
	base_node.Position = ring_positions[0]
	assert.Equal(t, base_node, manager.Nodes[0])

	base_node.Physical_Id = 1
	base_node.Id = 1
	base_node.Position = ring_positions[1]
	assert.Equal(t, base_node, manager.Nodes[1])

	base_node.Physical_Id = 2
	base_node.Id = 2
	base_node.Position = ring_positions[2]
	assert.Equal(t, base_node, manager.Nodes[2])

	base_node.Physical_Id = 0
	base_node.Id = 3
	base_node.Position = ring_positions[3]
	assert.Equal(t, base_node, manager.Nodes[3])

	base_node.Physical_Id = 1
	base_node.Id = 4
	base_node.Position = ring_positions[4]
	assert.Equal(t, base_node, manager.Nodes[4])

	base_node.Physical_Id = 2
	base_node.Id = 5
	base_node.Position = ring_positions[5]
	assert.Equal(t, base_node, manager.Nodes[5])

}

func TestAdd(t *testing.T) {
	var number_of_virtual_nodes int = 2
	var number_of_nodes int = 3
	var replication_factor int = 2
	var minimum_writes int = 2
	var minimum_read int = 2
	base_node := Node{}
	base_node.Address = "address"
	base_node.Http_node_address = "http address"
	base_node.Node_config_address = "config address"
	base_node.Http_port = 8080
	base_node.Internal_port = 6000

	manager := New(number_of_nodes, base_node, number_of_virtual_nodes, replication_factor, minimum_writes, minimum_read)

	manager.Add_Node(base_node, 3)

	ring_positions := hash_ring.Generate_Ring_Positions(9)

	//should be every 3rd index, as 9/3 = 3

	base_node.Physical_Id = 3
	base_node.Id = 6
	base_node.Position = ring_positions[0]
	assert.Equal(t, base_node, manager.Nodes[0])

	base_node.Physical_Id = 3
	base_node.Id = 7
	base_node.Position = ring_positions[3]
	assert.Equal(t, base_node, manager.Nodes[3])

	base_node.Physical_Id = 3
	base_node.Id = 8
	base_node.Position = ring_positions[6]
	assert.Equal(t, base_node, manager.Nodes[6])
}
