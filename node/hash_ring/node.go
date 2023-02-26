package hash_ring

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

func (n *Node) Add(key string, value string, meta *ValueMeta) error {
	return n.AddPermanent(key, value, meta)
}

func (n *Node) AddPermanent(key string, value string, meta *ValueMeta) error {
	return n.table.Add(key, value, meta)
}

func (n *Node) AddTemporary(key string, value string, meta *ValueMeta) error {
	return n.temporaryTable.Add(key, value, meta)
}

func (n *Node) GetEither(key string) (*string, *ValueMeta, error) {
	val, meta, err := n.GetPermanent(key)
	if err == nil && val != nil {
		return val, meta, err
	} else {
		return n.GetTemporary(key)
	}
}

func (n *Node) Get(key string, usePermanent bool) (*string, *ValueMeta, error) {
	if usePermanent {
		return n.GetPermanent(key)
	} else {
		return n.GetTemporary(key)
	}
}

func (n *Node) GetPermanent(key string) (*string, *ValueMeta, error) {
	return n.table.Get(key)
}

func (n *Node) GetTemporary(key string) (*string, *ValueMeta, error) {
	return n.temporaryTable.Get(key)
}
