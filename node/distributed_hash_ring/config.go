package distributed_hash_ring

import (
	"encoding/json"
	"os"

	"github.com/lucifer1662/distrokdb/node/hash_ring"
)

type Node struct {
	Position    hash_ring.KeyHash
	Address     string
	Id          uint64
	Physical_Id uint64
}

type SharedConfig struct {
	Nodes              []Node
	Replication_factor int
	Minimum_writes     int
	Minimum_read       int
}

type InstanceConfig struct {
	*SharedConfig
	My_id   uint64
	My_port int
}

func NewInstanceConfig(shared_config *SharedConfig,
	My_id uint64, My_port int) *InstanceConfig {
	return &InstanceConfig{*&shared_config, My_id, My_port}
}

func ReadConfig(path string) (*InstanceConfig, error) {
	config := InstanceConfig{}
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
