package hash_ring

import (
	"errors"
	"hash/fnv"
	"log"
	"sync"
)

func hash(s string) uint64 {
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

type KeyValueTable interface {
	Add(string, string) error
	Get(string) (*string, error)
	Remove(string) error
	Size() int
}

type Node struct {
	position KeyHash
	table    KeyValueTable
	id       uint64
	isActive bool
}

func (n *Node) Add(key string, value string) error {
	return n.table.Add(key, value)
}

func (n *Node) Get(key string) (*string, error) {
	return n.table.Get(key)
}

type Hash_Ring struct {
	nodes              []Node
	replication_factor int
	minimum_writes     int
	minimum_read       int
}

func Generate_Nodes(number_of_nodes uint64) []Node {
	nodes := make([]Node, number_of_nodes)

	node_size := MaxKeyHash / number_of_nodes

	for i := range nodes {
		node := &nodes[i]
		node.position = node_size * uint64(i+1)
	}

	nodes[len(nodes)-1].position = uint64(18446744073709551615)

	return nodes
}

func (ring *Hash_Ring) primary_node_index(keyHash KeyHash) int {
	for i, node := range ring.nodes {
		if node.position >= keyHash {
			return i
		}
	}
	return -1
}

// type Consensus struct {
// 	number_finished int
// 	number_running int
// 	next_node int
// 	lock sync.Mutex
// 	sync.WaitGroup wg
// 	hash_ring * Hash_Ring
// }

func (ring *Hash_Ring) wrapped_index(i int) int {
	return i - ((i / len(ring.nodes)) * len(ring.nodes))
}

func (ring *Hash_Ring) add(key string, value string, key_hash uint64) error {
	node_i := ring.primary_node_index(key_hash)
	if node_i == -1 {
		return errors.New("Missing node for key")
	}

	minimum_succeeded_chan := make(chan error)
	//start replicating data
	go func() {
		number_finished := 0
		result_chan := make(chan bool)
		wait_for_minimum_success_count := sync.WaitGroup{}
		wait_for_minimum_success_count.Add(ring.minimum_writes)

		i := 0
		//launch number of nodes as the replication factor
		for ; i < ring.replication_factor; i++ {
			node := &ring.nodes[node_i]
			node_i++
			if node_i == len(ring.nodes) {
				node_i = 0
			}
			go func() {
				err := node.Add(key, value)
				result_chan <- (err == nil)
			}()
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

			if number_finished == ring.minimum_writes && !sent_minimum_on_chan {
				minimum_succeeded_chan <- nil
				sent_minimum_on_chan = true
			}

			//ran out of nodes to replicate to
			if i == len(ring.nodes) {
				//replication failed
				log.Printf("Failed to replicate value to replication factor")
				//sometimes this will not propagate all the way up, as add() returns early
				minimum_succeeded_chan <- errors.New("Failed to replicate value to replication factor")
				return
			}

			//replicate to the replication factor, success
			if number_finished == ring.replication_factor {
				return
			}

			//node failed to replicate,
			if !succeeded {
				node := &ring.nodes[node_i]
				i++
				node_i++
				if node_i == len(ring.nodes) {
					node_i = 0
				}
				//launch new request, to a new node
				go func() {
					err := node.Add(key, value)
					result_chan <- (err == nil)
				}()

			}
		}

	}()

	return <-minimum_succeeded_chan
}

func (ring *Hash_Ring) Add(key string, value string) error {
	return ring.add(key, value, hash(key))
}

func (ring *Hash_Ring) consensus(key_hash KeyHash, minimum_for_early_return int, finish_early bool, node_op func(node *Node, result_chan chan bool))  (*string, error){
	node_i := ring.primary_node_index(key_hash)
	if node_i == -1 {
		return errors.New("Missing node for key")
	}
	
	minimum_succeeded_chan := make(chan error)
	//start replicating data
	go func() {
		number_finished := 0
		result_chan := make(chan bool)
		wait_for_minimum_success_count := sync.WaitGroup{}
		wait_for_minimum_success_count.Add(minimum_for_early_return)

		i := 0
		//launch number of nodes as the replication factor
		for ; i < ring.replication_factor; i++ {
			node := &ring.nodes[node_i]
			node_i++
			if node_i == len(ring.nodes) {
				node_i = 0
			}
			go node_op(node, result_chan);
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

			if number_finished == ring.minimum_read && !sent_minimum_on_chan {
				minimum_succeeded_chan <- nil
				sent_minimum_on_chan = true
				if finish_early {
					return
				}
			}

			//ran out of nodes to replicate to
			if i == len(ring.nodes) {
				//replication failed
				log.Printf("Failed to replicate value to replication factor")
				//sometimes this will not propagate all the way up, as add() returns early
				minimum_succeeded_chan <- errors.New("Failed to replicate value to replication factor")
				return
			}

			//replicate to the replication factor, success
			if number_finished == ring.replication_factor {
				return
			}

			//node failed to replicate,
			if !succeeded {
				node := &ring.nodes[node_i]
				i++
				node_i++
				if node_i == len(ring.nodes) {
					node_i = 0
				}
				//launch new request, to a new node
				go node_op(node, result_chan)
			}
		}
}
}


func (ring *Hash_Ring) get(key string, key_hash uint64) (*string, error) {
	// index := ring.primary_node_index(key_hash)
	// if index != -1 {
	// 	return ring.nodes[index].Get(key)
	// }
	// return nil, errors.New("Missing node for key")

	node_i := ring.primary_node_index(key_hash)
	if node_i == -1 {
		return errors.New("Missing node for key")
	}

	
	minimum_succeeded_chan := make(chan error)
	//start replicating data
	go func() {
		number_finished := 0
		result_chan := make(chan bool)
		wait_for_minimum_success_count := sync.WaitGroup{}
		wait_for_minimum_success_count.Add(ring.minimum_writes)

		i := 0
		//launch number of nodes as the replication factor
		for ; i < ring.replication_factor; i++ {
			node := &ring.nodes[node_i]
			node_i++
			if node_i == len(ring.nodes) {
				node_i = 0
			}
			go func() {
				err := node.Add(key, value)
				result_chan <- (err == nil)
			}()
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

			if number_finished == ring.minimum_read && !sent_minimum_on_chan {
				minimum_succeeded_chan <- nil
				sent_minimum_on_chan = true
				return
			}

			//ran out of nodes to replicate to
			if i == len(ring.nodes) {
				//replication failed
				log.Printf("Failed to replicate value to replication factor")
				//sometimes this will not propagate all the way up, as add() returns early
				minimum_succeeded_chan <- errors.New("Failed to replicate value to replication factor")
				return
			}

			//replicate to the replication factor, success
			if number_finished == ring.replication_factor {
				return
			}

			//node failed to replicate,
			if !succeeded {
				node := &ring.nodes[node_i]
				i++
				node_i++
				if node_i == len(ring.nodes) {
					node_i = 0
				}
				//launch new request, to a new node
				go func() {
					err := node.Add(key, value)
					result_chan <- (err == nil)
				}()

			}
		}
}

func (ring *Hash_Ring) Get(key string) (*string, error) {
	return ring.get(key, hash(key))
}
