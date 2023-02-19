package main

import (
	"flag"

	"github.com/lucifer1662/distrokdb/node/distributed_hash_ring"
	"github.com/lucifer1662/distrokdb/node/http_db_server"
	"github.com/lucifer1662/distrokdb/node/manager_server"
)

type DistributedKeyDataBase struct {
	hr_internal_server   *distributed_hash_ring.DistributedHashRingServer
	http_external_server *http_db_server.HttpDBServer
}

func NewDistributedKeyDataBase(config *manager_server.Config) *DistributedKeyDataBase {
	hr := distributed_hash_ring.New(config.Hash_ring_config)

	db := DistributedKeyDataBase{
		distributed_hash_ring.NewServer(&hr, config.Hash_ring_config.My_port),
		http_db_server.NewHttpDBServer(config.Http_config, &hr),
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

func main() {
	println("Started")

	config_port := flag.Int("config_port", 8312, "Will listen for a config on this port if no local config.json is found")
	config, err := manager_server.ReadConfig("./config.json", *config_port)
	if err != nil {
		println(err.Error())
		return
	}

	server := NewDistributedKeyDataBase(config)

	server.Start()
}
