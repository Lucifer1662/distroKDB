package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"luke/distrokdb/distributed_hash_ring"
	"luke/distrokdb/http_db_server"
	"net"
	"net/rpc"
	"os"
	"strconv"
)

type Config struct {
	Hash_ring_config *distributed_hash_ring.InstanceConfig
	Http_config      *http_db_server.Config
}

func read_config_from_file(path string) (*Config, error) {
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

func save_config(config *Config, path string) error {
	bytes, err := json.Marshal(config)
	if err != nil {
		return err
	}

	return ioutil.WriteFile(path, bytes, 0777)

}

func ReadConfig(path string, port int) (*Config, error) {
	config, err := read_config_from_file(path)

	if err == nil {
		return config, nil
	}

	config_server := newServer(port)

	config_server.Start()
	defer config_server.Stop()
	for {
		config = <-config_server.config_chan
		if config != nil {
			save_config(config, path)
			return config, nil
		}
	}

}

type ConfigServer struct {
	config_chan chan *Config
	rpc_server  *rpc.Server
	listener    *net.Listener
	address     string
}

func newServer(port int) *ConfigServer {
	rpc_server := rpc.NewServer()
	s := ConfigServer{make(chan *Config), rpc_server, nil, ":" + strconv.Itoa(port)}
	rpc_server.Register(&s)
	return &s
}

type SetConfig struct {
	config *Config
}

type SetConfigResponse struct {
	Success       bool
	Error_message string
}

func (t *ConfigServer) Add(request SetConfig, response *SetConfigResponse) error {
	var err error

	t.config_chan <- request.config

	response.Success = err == nil
	if !response.Success {
		response.Error_message = err.Error()
	}
	return err
}

func (server *ConfigServer) Start() {
	listener, e := net.Listen("tcp", server.address)
	server.listener = &listener
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go server.rpc_server.Accept(*server.listener)
}

func (server *ConfigServer) Stop() {
	if server.listener != nil {
		(*server.listener).Close()
	}
}
