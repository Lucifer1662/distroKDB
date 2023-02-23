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

type ValueMeta struct {
	VectorClock VectorClock
}

func (meta *ValueMeta) Copy() *ValueMeta {
	return &ValueMeta{
		VectorClock: meta.VectorClock.Copy(),
	}
}

func NewValueMeta(vectorClock VectorClock) *ValueMeta {
	return &ValueMeta{VectorClock: vectorClock}
}

type KeyValueIterator interface {
	Next() (*string, *string, *ValueMeta)
}

type KeyValueTable interface {
	Add(string, string, *ValueMeta) error
	Get(string) (*string, *ValueMeta, error)
	Size() int
	Iter() KeyValueIterator
	Erase(key string)
}

func CopyToMap(table KeyValueTable, data *map[string]string) {
	iter := table.Iter()
	for key, value, _ := iter.Next(); key != nil; key, value, _ = iter.Next() {
		(*data)[*key] = *value
	}
}

type ConflictResolution interface {
	Resolve(key string, values []*string, metas []*ValueMeta, nodes_position []uint64) *string
}

type Hash_Ring struct {
	nodes               []Node
	replication_factor  int
	minimum_writes      int
	minimum_read        int
	conflict_resolution ConflictResolution
	myId                uint64
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

func (ring *Hash_Ring) add(key string, value string, meta *ValueMeta, key_hash uint64) error {
	return ring.consensus(key_hash, ring.minimum_writes, false, func(node *Node, result_chan chan bool, hinted bool) {
		if hinted {
			err := node.AddTemporary(key, value, meta)
			result_chan <- (err == nil)
		} else {
			err := node.Add(key, value, meta)
			result_chan <- (err == nil)
		}

	})
}

func (ring *Hash_Ring) Add(key string, value string, meta *ValueMeta) error {
	new_meta := meta.Copy()
	new_meta.VectorClock.Counts[int(ring.myId)] = new_meta.VectorClock.Get(int(ring.myId)) + 1
	return ring.add(key, value, new_meta, Hash(key))
}

func (ring *Hash_Ring) resolveConflicts(node_id int, key string, value string, meta *ValueMeta) (string, *ValueMeta) {
	old_value, current_meta, _ := ring.nodes[node_id].Get(key)

	//if !(old -> new)
	if old_value != nil && IsNotCausal(&current_meta.VectorClock, &meta.VectorClock) {
		//need to resolve version
		new_value := ring.conflict_resolution.Resolve(key, []*string{old_value, &value}, []*ValueMeta{current_meta, meta}, []uint64{})
		new_meta := ValueMeta{
			VectorClock: MaxUpVectorClock(meta.VectorClock, current_meta.VectorClock),
		}
		return *new_value, &new_meta
	} else {
		return value, meta
	}
}

func (ring *Hash_Ring) AddToNodePermanent(node_position uint64, key string, value string, meta *ValueMeta) error {
	for i := range ring.nodes {
		if node_position == ring.nodes[i].position {
			new_value, new_meta := ring.resolveConflicts(i, key, value, meta)
			return ring.nodes[i].AddPermanent(key, new_value, new_meta)
		}
	}
	return errors.New("No node found")
}

func (ring *Hash_Ring) AddToNodeTemporary(node_position uint64, key string, value string, meta *ValueMeta) error {
	for i := range ring.nodes {
		if node_position == ring.nodes[i].position {
			new_value, new_meta := ring.resolveConflicts(i, key, value, meta)
			return ring.nodes[i].AddTemporary(key, new_value, new_meta)
		}
	}
	return errors.New("No node found")
}

func (ring *Hash_Ring) GetFromNodePermanent(node_position uint64, key string) (*string, *ValueMeta, error) {
	for i := range ring.nodes {
		if node_position == ring.nodes[i].position {
			return ring.nodes[i].GetPermanent(key)
		}
	}
	return nil, nil, errors.New("No node found")
}

func (ring *Hash_Ring) GetFromNodeTemporary(node_position uint64, key string) (*string, *ValueMeta, error) {
	for i := range ring.nodes {
		if node_position == ring.nodes[i].position {
			return ring.nodes[i].GetTemporary(key)
		}
	}
	return nil, nil, errors.New("No node found")
}

func (ring *Hash_Ring) ReplicateToPrimary(key string, value string, meta *ValueMeta) int {
	return ring.consensus_only_primary(Hash(key), func(node *Node, result_chan chan bool) {
		err := node.Add(key, value, meta)
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

func (ring *Hash_Ring) get(key string, key_hash uint64) (*string, *ValueMeta, error) {
	results := []*string{}
	metas := []*ValueMeta{}
	nodes_results := []uint64{}
	nodes_involved := []*Node{}
	was_primary := []bool{}
	lock := sync.Mutex{}

	err := ring.consensus(key_hash, ring.minimum_read, false, func(node *Node, result_chan chan bool, hinted bool) {
		var value *string
		var meta *ValueMeta
		var err error
		if hinted {
			value, meta, err = node.GetTemporary(key)
		} else {
			value, meta, err = node.GetPermanent(key)
		}

		if err == nil && value != nil {
			// if err == nil {
			lock.Lock()
			results = append(results, value)
			metas = append(metas, meta)
			nodes_results = append(nodes_results, node.position)
			nodes_involved = append(nodes_involved, node)
			was_primary = append(was_primary, !hinted)
			lock.Unlock()
		}
		result_chan <- (err == nil)
	})

	if err != nil {
		return nil, nil, err
	}

	if len(results) == 0 {
		return nil, NewValueMeta(NewVectorClock()), nil
	}

	leading_clocks := make([]*VectorClock, len(metas))
	for i := range metas {
		leading_clocks[i] = &metas[i].VectorClock
	}

	var latest_value *string
	var latest_meta *ValueMeta
	newest_casual_clock_index := FindLatestCasualVersion(leading_clocks)
	if newest_casual_clock_index != -1 {
		//no conflict resolution required
		latest_value = results[newest_casual_clock_index]
		latest_meta = metas[newest_casual_clock_index]

	} else {
		//Non casual relation found
		//Need to perform merge and create new leading version
		//perform merge
		latest_value = ring.conflict_resolution.Resolve(key, results, metas, nodes_results)

		//calculate newest version
		clocks := make([]VectorClock, len(metas))
		for i := range metas {
			clocks[i] = metas[i].VectorClock
		}
		new_clock := MaxUpVectorClocks(clocks)
		new_clock.Add(int(ring.myId))

		latest_meta = NewValueMeta(new_clock)
	}

	//should update old versions to latest version
	//will all be nil, if no head version is found
	for i := range leading_clocks {
		if leading_clocks[i] == nil {
			if was_primary[i] {
				nodes_involved[i].AddPermanent(key, *latest_value, latest_meta)
			} else {
				nodes_involved[i].AddTemporary(key, *latest_value, latest_meta)
			}
		}
	}

	return latest_value, latest_meta, nil
}

func (ring *Hash_Ring) Get(key string) (*string, *ValueMeta, error) {
	return ring.get(key, Hash(key))
}

func Cleanup_temporary(ring *Hash_Ring, temporaryTable KeyValueTable) {
	iter := temporaryTable.Iter()
	for key, value, meta := iter.Next(); key != nil; key, value, meta = iter.Next() {
		num_replicated_to := ring.ReplicateToPrimary(*key, *value, meta)
		if num_replicated_to == ring.replication_factor {
			//adheres to replication invariant, therefore can delete from temp
			temporaryTable.Erase(*key)
		}
	}
}
