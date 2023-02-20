package cluster_manager

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/rpc"
	"os"
	"sync"

	"github.com/lucifer1662/distrokdb/node/distributed_hash_ring"
	"github.com/lucifer1662/distrokdb/node/hash_ring"
	"github.com/lucifer1662/distrokdb/node/http_db_server"
	"github.com/lucifer1662/distrokdb/node/manager_server"
)

type SetConfig struct {
	config *manager_server.Config
}

type SetConfigResponse struct {
	Success       bool
	Error_message string
}

func SetConfigOnNode(config *manager_server.Config, server_address string) (*SetConfigResponse, error) {
	client, err := rpc.Dial("tcp", server_address)
	if err != nil {
		return nil, err
	}

	// Synchronous call
	args := &SetConfig{config}
	var reply SetConfigResponse

	//blocks for response
	err = client.Call("DistributedHashRingServer.Get", args, &reply)
	if err != nil {
		return nil, err
	}
	return &reply, nil
}

type Node struct {
	Position            hash_ring.KeyHash
	Address             string
	Internal_port       int
	Http_port           int
	Id                  uint64
	Physical_Id         uint64
	Http_node_address   string
	Node_config_address string
}

type ClusterManager struct {
	Nodes                 []Node
	Replication_factor    int
	Minimum_writes        int
	Minimum_read          int
	Next_physical_node_id uint64
	Next_node_id          uint64
}

func insert(a []int, index int, value int) []int {
	if len(a) == index { // nil or empty slice or after last element
		return append(a, value)
	}
	a = append(a[:index+1], a[index:]...) // index < len(a)
	a[index] = value
	return a
}

func (manager *ClusterManager) Add_Node(
	base_node  Node,
	number_of_virtual_nodes int) {

	base_node.Physical_Id = manager.Next_physical_node_id
	manager.Next_physical_node_id++


	new_number_of_nodes := len(manager.Nodes) + number_of_virtual_nodes

	new_nodes := make([]Node, new_number_of_nodes)
	virtual_node_spacing := int(new_number_of_nodes / number_of_virtual_nodes)
	j := 0
	for i := 0; i < new_number_of_nodes; i++ {
		if i%virtual_node_spacing == 0 {
			base_node.Id = manager.Next_node_id
			manager.Next_node_id++
			new_nodes[i] = base_node
		} else {
			new_nodes[i] = manager.Nodes[j]
			j++
		}
	}

	ring_positions := hash_ring.Generate_Ring_Positions(new_number_of_nodes)
	for i := range ring_positions {
		manager.Nodes[i].Position = ring_positions[i]
	}

}

func New(
	number_of_nodes int,
	base_node  Node,
	number_of_virtual_nodes int) ClusterManager {

	new_nodes := make([]Node, number_of_nodes)
	for i := 0; i < number_of_virtual_nodes; i++ {
		for j := 0; j < number_of_nodes; j++ {
			base_node.Id = manager.Next_node_id
			base_node.Physical_Id = j
			manager.Next_node_id++
			new_nodes[i * number_of_nodes + j] = base_node
		}
	}

	ring_positions := hash_ring.Generate_Ring_Positions(new_number_of_nodes)
	for i := range ring_positions {
		new_nodes[i].Position = ring_positions[i]
	}

	return ClusterManager{
		Nodes: new_nodes,
		Replication_factor: Replication_factor,
		Minimum_writes: minimum_writes,
		Minimum_read: Minimum_read,
		Next_physical_node_id: uint64(number_of_nodes),
		Next_node_id: len(new_nodes),

	}

}

/*
func (manager *ClusterManager) Add_Node(
	new_node_config_address string,
	new_node_internal_port int,
	new_node_external_address string,
	new_node_internal_http_port int,
	new_node_external_http_address string,
	number_of_virtual_nodes int) {
	new_number_of_nodes := len(manager.SharedConfig.Nodes) + number_of_virtual_nodes

	ring_positions := hash_ring.Generate_Ring_Positions(new_number_of_nodes)
	node_indices := make([]int, new_number_of_nodes)

	for i := 0; i < new_number_of_nodes; i++ {
		nod
	}

	for i := 0; i < number_of_virtual_nodes; i++ {
		id := manager.Next_node_id
		manager.Next_node_id++
		manager.SharedConfig.Nodes = append(manager.SharedConfig.Nodes, distributed_hash_ring.Node{
			Address:     new_node_external_address,
			Physical_Id: manager.Next_physical_node_id,
			Id:          id,
		})
		manager.Nodes = append(manager.Nodes, manager_server.Config{
			Hash_ring_config: &distributed_hash_ring.InstanceConfig{
				SharedConfig: &manager.SharedConfig,
				My_port:      new_node_internal_port,
				My_id:        id,
			},
			Http_config: &http_db_server.Config{
				My_id:     id,
				Http_port: new_node_internal_http_port,
			},
		})

		manager.Http_node_addresses = append(manager.Http_node_addresses, new_node_external_http_address)
		manager.Node_config_address = append(manager.Node_config_address, new_node_config_address)
	}

	for i := range ring_positions {
		manager.SharedConfig.Nodes[i].Position = ring_positions[i]
	}

	manager.Next_physical_node_id++
}
*/

func (manager *ClusterManager) UpdateConfigs() {
	wg := sync.WaitGroup{}
	wg.Add(len(manager.Nodes))

	shared_config := distributed_hash_ring.SharedConfig{
		Replication_factor: manager.Replication_factor,
		Minimum_writes:     manager.Minimum_writes,
		Minimum_read:       manager.Minimum_read,
		Nodes:              make([]distributed_hash_ring.Node, len(manager.Nodes)),
	}

	for i := 0; i < len(shared_config.Nodes); i++ {
		shared_config.Nodes[i].Address = manager.Nodes[i].Address
		shared_config.Nodes[i].Id = manager.Nodes[i].Id
		shared_config.Nodes[i].Physical_Id = manager.Nodes[i].Physical_Id
		shared_config.Nodes[i].Position = manager.Nodes[i].Position
	}

	for i := range manager.Nodes {
		my_index := i
		go func() {
			defer wg.Done()
			SetConfigOnNode(&manager_server.Config{
				Http_config: &http_db_server.Config{
					My_id:     manager.Nodes[my_index].Id,
					Http_port: manager.Nodes[my_index].Http_port,
				},
				Hash_ring_config: &distributed_hash_ring.InstanceConfig{
					My_port:      manager.Nodes[my_index].Internal_port,
					My_id:        manager.Nodes[my_index].Id,
					SharedConfig: &shared_config,
				},
			},
				manager.Nodes[my_index].Node_config_address,
			)
		}()
	}

	wg.Wait()
}

func SaveClusterManagerState(path string, cluster_manager *ClusterManager) error {
	data, err := json.Marshal(&cluster_manager)
	if err != nil {
		return err
	}

	err = os.WriteFile(path, data, 0777)
	if err != nil {
		return err
	}

	return nil
}

func ReadClusterManager(path string) (*ClusterManager, error) {
	config := ClusterManager{}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(data, &config)
	if err != nil {
		return nil, err
	}

	return &config, nil
}

func main() {
	manager, err := ReadClusterManager("cluster_manager.json")

	if err != nil {
		println(err.Error())
	}

	switch os.Args[1] {
	case "init":
		var new_node_config_address string
		var new_node_external_address string
		var new_node_external_http_address string
		var internal_port int
		var new_node_internal_http_port int
		var number_of_virtual_nodes int
		flag.StringVar(&new_node_config_address, "config_address", "", "The external address that the node will listen on for management information")
		flag.StringVar(&new_node_external_address, "public_address", "", "The public address that the node will communicate with other nodes directly")
		flag.StringVar(&new_node_external_http_address, "external_http_port", "", "The port the node will listen on for communication between nodes, may be different to the public_address if the system is using proxies or docker containers")

		flag.IntVar(&internal_port, "node_port", 6023, "The port the node will listen on for communication between nodes, may be different to the public_address if the system is using proxies or docker containers")
		flag.IntVar(&new_node_internal_http_port, "http_port", 8080, "The port the node will listen on to accept http request from clients")
		flag.IntVar(&number_of_virtual_nodes, "number_virtual_nodes", 1, "The number of virtual nodes for this physical node")
		manager.Add_Node(new_node_config_address, internal_port, new_node_external_address, new_node_internal_http_port, new_node_external_http_address, number_of_virtual_nodes)
		fmt.Println("one")
	}
	case "add":
		var new_node_config_address string
		var new_node_external_address string
		var new_node_external_http_address string
		var internal_port int
		var new_node_internal_http_port int
		var number_of_virtual_nodes int
		flag.StringVar(&new_node_config_address, "config_address", "", "The external address that the node will listen on for management information")
		flag.StringVar(&new_node_external_address, "public_address", "", "The public address that the node will communicate with other nodes directly")
		flag.StringVar(&new_node_external_http_address, "external_http_port", "", "The port the node will listen on for communication between nodes, may be different to the public_address if the system is using proxies or docker containers")

		flag.IntVar(&internal_port, "node_port", 6023, "The port the node will listen on for communication between nodes, may be different to the public_address if the system is using proxies or docker containers")
		flag.IntVar(&new_node_internal_http_port, "http_port", 8080, "The port the node will listen on to accept http request from clients")
		flag.IntVar(&number_of_virtual_nodes, "number_virtual_nodes", 1, "The number of virtual nodes for this physical node")
		manager.Add_Node(new_node_config_address, internal_port, new_node_external_address, new_node_internal_http_port, new_node_external_http_address, number_of_virtual_nodes)
		fmt.Println("one")
	}

}
