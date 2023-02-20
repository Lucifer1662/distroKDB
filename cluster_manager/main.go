package cluster_manager

import (
	"encoding/json"
	"net/rpc"
	"os"

	"github.com/lucifer1662/distrokdb/node/distributed_hash_ring"
	"github.com/lucifer1662/distrokdb/node/hash_ring"
	"github.com/lucifer1662/distrokdb/node/manager_server"
)

type Config struct {
	Node_addresses []string
	Nodes          []manager_server.Config
}

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

func ReadConfig(path string) (*Config, error) {
	config := Config{}
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

func SaveConfig(path string, config *Config) error {
	data, err := json.Marshal(&config)
	if err != nil {
		return err
	}

	err = os.WriteFile(path, data, 0777)
	if err != nil {
		return err
	}

	return nil
}

type ClusterManager struct {
	SharedConfig distributed_hash_ring.SharedConfig
	Nodes        []manager_server.Config
}

func (manager *ClusterManager) Add_Node(new_node_config_address string, new_node_internal_address string, new_node_internal_port int, number_of_virtual_nodes int) {
	new_number_of_nodes := len(manager.SharedConfig.Nodes)

	ring_positions := hash_ring.Generate_Ring_Positions(new_number_of_nodes)

	for i := 0; i < number_of_virtual_nodes; i++ {
		manager.SharedConfig.Nodes = append(manager.SharedConfig.Nodes, distributed_hash_ring.Node{
			Address: new_node_internal_address,
		})
		manager.Nodes = append(manager.Nodes, manager_server.Config{
			Hash_ring_config: &distributed_hash_ring.InstanceConfig{
				SharedConfig: &manager.SharedConfig,
				My_port:      new_node_internal_port,
			},
		})

	}

	for i := range ring_positions {
		manager.SharedConfig.Nodes[i].Position = ring_positions[i]
		manager.SharedConfig.Nodes[i].Id = uint64(i)
	}

}

func main() {

}
