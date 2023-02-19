package hash_ring

import (
	"errors"
	"hash/fnv"
	"log"
	"sync"
)

func Hash(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}

type KeyHash = uint64

const MaxKeyHash = ^KeyHash(0)

type KeyHashRange struct {
	start KeyHash
	end   KeyHash
}

func (r KeyHashRange) contains(key_hash KeyHash) bool {
	return r.start <= key_hash && key_hash <= r.end
}

type KeyValueIterator interface {
	Next() (*string, *string)
}

type KeyValueTable interface {
	Add(string, string) error
	Get(string) (*string, error)
	Size() int
	Iter() KeyValueIterator
	Erase(key string)
}

func CopyToMap(table KeyValueTable, data *map[string]string) {
	iter := table.Iter()
	for key, value := iter.Next(); key != nil; key, value = iter.Next() {
		(*data)[*key] = *value
	}
}

type Node struct {
	position       KeyHash
	table          KeyValueTable
	temporaryTable KeyValueTable
	physical_id    uint64
}

func NewNode(position KeyHash, table KeyValueTable, temporaryTable KeyValueTable, physical_id uint64) Node {
	return Node{position, table, temporaryTable, physical_id}
}

func (n *Node) GetTable() KeyValueTable {
	return n.table
}

func (n *Node) GetTemporaryTable() KeyValueTable {
	return n.temporaryTable
}

func (n *Node) GetPosition() KeyHash {
	return n.position
}

func (n *Node) GetPhysicalId() uint64 {
	return n.physical_id
}

func (n *Node) SetTable(table KeyValueTable) {
	n.table = table
}

func (n *Node) SetTemporaryTable(temporaryTable KeyValueTable) {
	n.temporaryTable = temporaryTable
}

func (n *Node) Add(key string, value string) error {
	return n.AddPermanent(key, value)
}

func (n *Node) AddPermanent(key string, value string) error {
	return n.table.Add(key, value)
}

func (n *Node) AddTemporary(key string, value string) error {
	return n.temporaryTable.Add(key, value)
}

func (n *Node) Get(key string) (*string, error) {
	val, err := n.GetPermanent(key)
	if err == nil && val != nil {
		return val, err
	} else {
		return n.GetTemporary(key)
	}
}

func (n *Node) GetPermanent(key string) (*string, error) {
	return n.table.Get(key)
}

func (n *Node) GetTemporary(key string) (*string, error) {
	return n.temporaryTable.Get(key)
}

type ConflictResolution interface {
	Resolve(key string, values []*string, nodes_position []uint64) int
}

type Hash_Ring struct {
	nodes               []Node
	replication_factor  int
	minimum_writes      int
	minimum_read        int
	conflict_resolution ConflictResolution
}

func New(nodes []Node,
	replication_factor int,
	minimum_writes int,
	minimum_read int,
	conflict_resolution ConflictResolution) Hash_Ring {
	return Hash_Ring{
		nodes:               nodes,
		replication_factor:  replication_factor,
		minimum_writes:      minimum_writes,
		minimum_read:        minimum_read,
		conflict_resolution: conflict_resolution,
	}
}

func (hr *Hash_Ring) Nodes() []Node {
	return hr.nodes
}

func Generate_Nodes_With_Virtual(number_of_physical_nodes int, virtual_nodes_counts []int) []Node {
	count := 0
	for i := 0; i < number_of_physical_nodes; i++ {
		count += virtual_nodes_counts[i]
	}

	nodes := Generate_Nodes(count)

	index := 0
	for i := 0; i < number_of_physical_nodes; i++ {
		for j := 0; j < virtual_nodes_counts[i]; j++ {
			//assign all these nodes, to this physical node physical_id
			nodes[index].physical_id = uint64(i)
			index++
		}
	}

	return nodes
}

func Generate_Nodes(number_of_nodes int) []Node {
	nodes := make([]Node, number_of_nodes)

	node_size := MaxKeyHash / uint64(number_of_nodes)

	for i := range nodes {
		node := &nodes[i]
		node.position = node_size * uint64(i+1)
		//node == machine, no virtual nodes
		node.physical_id = uint64(i)
	}

	nodes[len(nodes)-1].position = uint64(18446744073709551615)

	return nodes
}

func Generate_Ring_Positions(number_of_nodes int) []KeyHash {
	hashes := make([]KeyHash, number_of_nodes)

	node_size := MaxKeyHash / uint64(number_of_nodes)

	for i := range hashes {
		hashes[i] = node_size * uint64(i+1)
	}

	hashes[len(hashes)-1] = uint64(18446744073709551615)

	return hashes
}

func (ring *Hash_Ring) primary_node_index(keyHash KeyHash) int {
	for i, node := range ring.nodes {
		if node.position >= keyHash {
			return i
		}
	}
	return -1
}

func (ring *Hash_Ring) wrapped_index(i int) int {
	return i - ((i / len(ring.nodes)) * len(ring.nodes))
}

func (ring *Hash_Ring) add(key string, value string, key_hash uint64) error {
	return ring.consensus(key_hash, ring.minimum_writes, false, func(node *Node, result_chan chan bool, hinted bool) {
		if hinted {
			err := node.AddTemporary(key, value)
			result_chan <- (err == nil)
		} else {
			err := node.Add(key, value)
			result_chan <- (err == nil)
		}

	})
}

func (ring *Hash_Ring) Add(key string, value string) error {
	return ring.add(key, value, Hash(key))
}

func (ring *Hash_Ring) AddToNodePermanent(node_position uint64, key string, value string) error {
	for i := range ring.nodes {
		if node_position == ring.nodes[i].position {
			return ring.nodes[i].AddPermanent(key, value)
		}
	}
	return errors.New("No node found")
}

func (ring *Hash_Ring) AddToNodeTemporary(node_position uint64, key string, value string) error {
	for i := range ring.nodes {
		if node_position == ring.nodes[i].position {
			return ring.nodes[i].AddTemporary(key, value)
		}
	}
	return errors.New("No node found")
}

func (ring *Hash_Ring) GetFromNodePermanent(node_position uint64, key string) (*string, error) {
	for i := range ring.nodes {
		if node_position == ring.nodes[i].position {
			return ring.nodes[i].GetPermanent(key)
		}
	}
	return nil, errors.New("No node found")
}

func (ring *Hash_Ring) GetFromNodeTemporary(node_position uint64, key string) (*string, error) {
	for i := range ring.nodes {
		if node_position == ring.nodes[i].position {
			return ring.nodes[i].GetTemporary(key)
		}
	}
	return nil, errors.New("No node found")
}

func (ring *Hash_Ring) ReplicateToPrimary(key string, value string) int {
	return ring.consensus_only_primary(Hash(key), func(node *Node, result_chan chan bool) {
		err := node.Add(key, value)
		result_chan <- (err == nil)
	})
}

func (ring *Hash_Ring) consensus_only_primary(key_hash KeyHash, node_op func(node *Node, result_chan chan bool)) int {
	node_i := ring.primary_node_index(key_hash)
	physical_nodes_visited := make(map[uint64]bool)
	if node_i == -1 {
		return -1
	}

	number_finished := 0
	result_chan := make(chan bool)

	nodes_started := 0
	nodes_inspected := 0

	request_node := func() bool {
		for nodes_inspected < len(ring.nodes) {
			node := &ring.nodes[node_i]
			node_i++
			if node_i == len(ring.nodes) {
				node_i = 0
			}

			_, exists := physical_nodes_visited[node.physical_id]
			if !exists {
				physical_nodes_visited[node.physical_id] = true
				nodes_started++
				go node_op(node, result_chan)
				return true
			}
			nodes_inspected++
		}
		return false
	}

	//launch number of nodes as the replication factor
	for nodes_started < ring.replication_factor {
		if !request_node() {
			break
		}
	}

	for i := 0; i < nodes_started; i++ {
		succeeded := <-result_chan
		if succeeded {
			number_finished += 1
		}
	}

	return number_finished
}

func (ring *Hash_Ring) consensus(key_hash KeyHash, minimum_for_early_return int, finish_early bool, node_op func(node *Node, result_chan chan bool, hinted bool)) error {
	node_i := ring.primary_node_index(key_hash)
	physical_nodes_visited := make(map[uint64]bool)
	if node_i == -1 {
		return errors.New("Missing node for key")
	}

	minimum_succeeded_chan := make(chan error)
	//start replicating data
	go func() {
		number_finished := 0
		result_chan := make(chan bool)

		nodes_started := 0
		nodes_inspected := 0

		request_node := func(hinted bool) bool {
			for nodes_inspected < len(ring.nodes) {
				node := &ring.nodes[node_i]
				node_i++
				if node_i == len(ring.nodes) {
					node_i = 0
				}

				_, exists := physical_nodes_visited[node.physical_id]
				if !exists {
					physical_nodes_visited[node.physical_id] = true
					nodes_started++
					go node_op(node, result_chan, hinted)
					return true
				}
				nodes_inspected++
			}
			//replication failed
			log.Printf("Failed to replicate value to replication factor")
			//sometimes this will not propagate all the way up, as add() returns early
			minimum_succeeded_chan <- errors.New("Failed to replicate value to replication factor")
			return false
		}

		//launch number of nodes as the replication factor
		for nodes_started < ring.replication_factor {
			if !request_node(false) {
				//replication failed
				return
			}
		}

		sent_minimum_on_chan := false

		//wait for request to fail
		//if fail one more iteration of loop
		//if success increment number finished
		for {
			succeeded := <-result_chan
			if succeeded {
				number_finished += 1
			}

			if number_finished == minimum_for_early_return && !sent_minimum_on_chan {
				minimum_succeeded_chan <- nil
				sent_minimum_on_chan = true
				if finish_early {
					return
				}
			}

			//replicate to the replication factor, success
			if number_finished == ring.replication_factor {
				return
			}

			//node failed to replicate,
			if !succeeded {
				if !request_node(true) {
					//replication failed
					return
				}
			}
		}
	}()
	return <-minimum_succeeded_chan
}

func (ring *Hash_Ring) get(key string, key_hash uint64) (*string, error) {
	results := []*string{}
	nodes_results := []uint64{}
	lock := sync.Mutex{}

	err := ring.consensus(key_hash, ring.minimum_read, false, func(node *Node, result_chan chan bool, hinted bool) {
		var value *string
		var err error
		if hinted {
			value, err = node.GetTemporary(key)
		} else {
			value, err = node.GetPermanent(key)
		}

		result_chan <- (err == nil)
		if err == nil && value != nil {
			lock.Lock()
			results = append(results, value)
			nodes_results = append(nodes_results, node.position)
			lock.Unlock()
		}
	})

	if err != nil {
		return nil, err
	}

	//perform merge
	selection := ring.conflict_resolution.Resolve(key, results, nodes_results)

	if selection < 0 || selection >= len(results) {
		return nil, errors.New("Could not resolve conflicting merge")
	}

	return results[selection], err
}

func (ring *Hash_Ring) Get(key string) (*string, error) {
	return ring.get(key, Hash(key))
}

// func (ring *Hash_Ring) Cleanup_temporary() {
// 	for i := range ring.nodes {
// 		node := &ring.nodes[i]
// 		iter := node.temporaryTable.Iter()
// 		for key, value := iter.Next(); key != nil; key, value = iter.Next() {
// 			num_replicated_to := ring.ReplicateToPrimary(*key, *value)
// 			if num_replicated_to == ring.replication_factor {
// 				//adheres to replication invariant, therefore can delete from temp
// 				node.temporaryTable.Erase(*key)
// 			}
// 		}
// 	}
// }

func Cleanup_temporary(ring *Hash_Ring, temporaryTable KeyValueTable) {
	iter := temporaryTable.Iter()
	for key, value := iter.Next(); key != nil; key, value = iter.Next() {
		num_replicated_to := ring.ReplicateToPrimary(*key, *value)
		if num_replicated_to == ring.replication_factor {
			//adheres to replication invariant, therefore can delete from temp
			temporaryTable.Erase(*key)
		}
	}
}
