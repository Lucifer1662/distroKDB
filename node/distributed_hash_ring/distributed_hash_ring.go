package distributed_hash_ring

import "github.com/lucifer1662/distrokdb/node/hash_ring"

func New(config *InstanceConfig) hash_ring.Hash_Ring {
	nodes := make([]hash_ring.Node, len(config.Nodes))

	//share all temporary data
	in_memory_temp_table := hash_ring.NewInMemoryTable()

	for i := range nodes {
		node := &config.Nodes[i]
		is_me := node.Id == config.My_id
		var permTable hash_ring.KeyValueTable
		var temporaryTable hash_ring.KeyValueTable

		if is_me {
			mem_table1 := hash_ring.NewInMemoryTable()
			permTable = &LocalTable{&mem_table1}
			temporaryTable = &in_memory_temp_table
		} else {
			permTable = &DistributedTable{node.Address, node.Position}
			//temporary Table should never be directly accessed from distributed source
			temporaryTable = &hash_ring.EmptyTable{}
		}

		nodes[i] = hash_ring.NewNode(node.Position, permTable, temporaryTable, node.Physical_Id)
	}

	return hash_ring.New(nodes, config.Replication_factor, config.Minimum_writes, config.Minimum_read, &hash_ring.ConflictResolutionFirstInstance{})
}
