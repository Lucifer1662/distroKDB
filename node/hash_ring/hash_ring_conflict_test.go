package hash_ring

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetOfNonExistentValue(t *testing.T) {
	hr := Hash_Ring{Generate_Nodes(1), 1, 1, 1, &ConflictResolutionFirstInstance{}, 0}
	for i := range hr.nodes {
		table := NewInMemoryTable()
		hr.nodes[i].table = &table
		tempTable := NewInMemoryTable()
		hr.nodes[i].temporaryTable = &tempTable
	}

	var nil_string *string = nil
	value, meta, err := hr.Get("foo")
	assert.Nil(t, err)
	assert.Equal(t, nil_string, value)
	assert.Equal(t, NewVectorClock(), meta.VectorClock)

}

func TestPutNoConflict(t *testing.T) {
	hr := Hash_Ring{Generate_Nodes(1), 1, 1, 1, &ConflictResolutionFirstInstance{}, 0}
	for i := range hr.nodes {
		table := NewInMemoryTable()
		hr.nodes[i].table = &table
		tempTable := NewInMemoryTable()
		hr.nodes[i].temporaryTable = &tempTable
	}

	//get foo -> ""
	//add foo moo
	//get foo -> "moo"

	meta := NewValueMeta(NewVectorClock())
	hr.Add("foo", "moo", meta)

	value, get_meta, err := hr.Get("foo")
	assert.Nil(t, err)
	assert.Equal(t, "moo", *value)
	assert.Equal(t, VectorClock{Counts: map[int]int{0: 1}}, get_meta.VectorClock)
}

func TestPutConflictSameNode(t *testing.T) {
	resolution := &SavePositionConflictResolution{[]uint64{}, []string{}, false}

	hr := Hash_Ring{Generate_Nodes(1), 1, 1, 1, resolution, 0}
	for i := range hr.nodes {
		table := NewInMemoryTable()
		hr.nodes[i].table = &table
		tempTable := NewInMemoryTable()
		hr.nodes[i].temporaryTable = &tempTable
	}

	//   1:1                     2:1
	//get foo -> "" ()        get foo -> "" ()
	//add foo moo ()->(0:1)   add foo car ()->(0:1)
	//    resolution picks alphabetically
	//		  get foo -> car (0:2)

	meta := NewValueMeta(NewVectorClock())
	hr.Add("foo", "moo", meta)

	value, get_meta, err := hr.Get("foo")
	assert.Nil(t, err)
	assert.Equal(t, "moo", *value)
	assert.Equal(t, VectorClock{Counts: map[int]int{0: 1}}, get_meta.VectorClock)

	hr.Add("foo", "car", meta)

	assert.Equal(t, true, resolution.Was_Called)

	value, get_meta, err = hr.Get("foo")
	assert.Nil(t, err)
	assert.Equal(t, "car", *value)
	assert.Equal(t, VectorClock{Counts: map[int]int{0: 2}}, get_meta.VectorClock)
}

func TestPutConflictDifferentNode(t *testing.T) {
	resolution := &SavePositionConflictResolution{[]uint64{}, []string{}, false}

	positions := Generate_Ring_Positions(2)

	hr1 := Hash_Ring{Generate_Nodes(2), 2, 2, 2, resolution, 0}
	hr2 := Hash_Ring{Generate_Nodes(2), 2, 2, 2, resolution, 1}
	permTable1 := NewInMemoryTable()
	permTable2 := NewInMemoryTable()
	tempTable1 := NewInMemoryTable()
	tempTable2 := NewInMemoryTable()

	permProxyTable1 := ProxyTable{hr: &hr1, table: &permTable1, key_position: positions[0], lock: sync.Mutex{}, isPermanent: true}
	tempProxyTable1 := ProxyTable{hr: &hr1, table: &tempTable1, key_position: positions[0], lock: sync.Mutex{}, isPermanent: false}

	permProxyTable2 := ProxyTable{hr: &hr2, table: &permTable2, key_position: positions[1], lock: sync.Mutex{}, isPermanent: true}
	tempProxyTable2 := ProxyTable{hr: &hr2, table: &tempTable2, key_position: positions[1], lock: sync.Mutex{}, isPermanent: false}

	//real tables
	hr1.nodes[0].table = &permTable1
	hr1.nodes[0].temporaryTable = &tempTable1

	hr1.nodes[1].table = &permProxyTable2
	hr1.nodes[1].temporaryTable = &tempProxyTable2

	//proxy tables
	hr2.nodes[0].table = &permProxyTable1
	hr2.nodes[0].temporaryTable = &tempProxyTable1

	hr2.nodes[1].table = &permTable2
	hr2.nodes[1].temporaryTable = &tempTable2

	//   1:1               2:2
	//get foo -> ""   get foo -> ""
	//add foo moo     add foo car
	//    resolution picks alphabetically
	//		  get foo -> car

	meta := NewValueMeta(NewVectorClock())
	hr1.Add("foo", "moo", meta)
	hr2.Add("foo", "car", meta)

	value, get_meta, err := hr1.Get("foo")
	assert.Equal(t, true, resolution.Was_Called)
	assert.Nil(t, err)
	assert.Equal(t, "car", *value)
	assert.Equal(t, VectorClock{Counts: map[int]int{0: 2}}, get_meta.VectorClock)

	resolution.Was_Called = false

	//value should have been replicated to hr2
	value, get_meta, err = hr2.Get("foo")
	assert.Equal(t, false, resolution.Was_Called)
	assert.Nil(t, err)
	assert.Equal(t, "car", *value)
	assert.Equal(t, VectorClock{Counts: map[int]int{0: 2}}, get_meta.VectorClock)
}
