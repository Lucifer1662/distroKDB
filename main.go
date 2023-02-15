package main

import (
	"encoding/json"
	"luke/distrokdb/distributed_hash_ring"
	"luke/distrokdb/http_db_server"
	"os"
)

type DistributedKeyDataBase struct {
	hr_internal_server   *distributed_hash_ring.DistributedHashRingServer
	http_external_server *http_db_server.HttpDBServer
}

type Config struct {
	Hash_ring_config *distributed_hash_ring.InstanceConfig
	http_config      *http_db_server.Config
}

func NewDistributedKeyDataBase(config *Config) *DistributedKeyDataBase {
	hr := distributed_hash_ring.New(config.Hash_ring_config)

	db := DistributedKeyDataBase{
		distributed_hash_ring.NewServer(&hr, config.Hash_ring_config.My_port),
		http_db_server.NewHttpDBServer(config.http_config, &hr),
	}

	return &db
}

func (db *DistributedKeyDataBase) Stop() {
	db.hr_internal_server.Stop()
	db.http_external_server.Stop()

}

func (db *DistributedKeyDataBase) Start() {

	go func() {
		db.hr_internal_server.Start()
	}()

	go func() {
		db.http_external_server.Start()
	}()
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

func main() {
	config, err := ReadConfig("./config.json")
	if err != nil {
		println(err)
		return
	}

	server := NewDistributedKeyDataBase(config)

	server.Start()
}
