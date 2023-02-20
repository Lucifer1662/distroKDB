package main

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"

	"github.com/lucifer1662/distrokdb/node/distributed_hash_ring"
	"github.com/lucifer1662/distrokdb/node/hash_ring"
	"github.com/lucifer1662/distrokdb/node/http_db_server"
	"github.com/lucifer1662/distrokdb/node/manager_server"

	"github.com/stretchr/testify/assert"
)

func create_two_node_setup(replication_factor int, min_writes int, min_reads int) (*DistributedKeyDataBase, *DistributedKeyDataBase) {
	positions := hash_ring.Generate_Ring_Positions(2)

	shared_config := distributed_hash_ring.SharedConfig{
		Nodes: []distributed_hash_ring.Node{
			{
				Position:    positions[0],
				Address:     "localhost:1234",
				Id:          0,
				Physical_Id: 0,
			},
			{
				Position:    positions[1],
				Address:     "localhost:1235",
				Id:          1,
				Physical_Id: 1,
			},
		},
		Replication_factor: replication_factor,
		Minimum_writes:     min_writes,
		Minimum_read:       min_reads,
	}

	config1 := manager_server.Config{
		Hash_ring_config: distributed_hash_ring.NewInstanceConfig(&shared_config, 0, 1234),
		Http_config: &http_db_server.Config{
			Http_port: 3000,
			My_id:     0,
		},
	}

	config2 := manager_server.Config{
		Hash_ring_config: distributed_hash_ring.NewInstanceConfig(&shared_config, 1, 1235),
		Http_config: &http_db_server.Config{
			Http_port: 3001,
			My_id:     1,
		},
	}

	bytes, _ := json.Marshal(config1)
	println(bytes)

	server1 := NewDistributedKeyDataBase(&config1)
	server2 := NewDistributedKeyDataBase(&config2)

	return server1, server2
}

func TestDistributedKeyDataBase(t *testing.T) {
	server1, server2 := create_two_node_setup(1, 1, 1)

	server1.Start()
	server2.Start()

	{
		response, err := http.Post("http://localhost:3000/add?key=bar&value=bar", "", strings.NewReader(""))
		assert.Nil(t, err)
		assert.Equal(t, "200 OK", response.Status)
	}

	{
		response, err := http.Get("http://localhost:3000/get?key=bar")
		assert.Nil(t, err)
		bytes, read_err := ioutil.ReadAll(response.Body)
		assert.Nil(t, read_err)
		var value string
		json_err := json.Unmarshal(bytes, &value)
		assert.Nil(t, json_err)
		assert.NotNil(t, value)
		assert.Equal(t, "bar", value)
	}

	{
		response, err := http.Get("http://localhost:3001/get?key=bar")
		assert.Nil(t, err)
		bytes, read_err := ioutil.ReadAll(response.Body)
		assert.Nil(t, read_err)
		var value string
		json_err := json.Unmarshal(bytes, &value)
		assert.Nil(t, json_err)
		assert.NotNil(t, value)
		assert.Equal(t, "bar", value)
	}
}

func TestGetAll(t *testing.T) {
	server1, server2 := create_two_node_setup(1, 1, 1)

	server1.Start()
	server2.Start()

	{
		response, err := http.Post("http://localhost:3000/add?key=bar&value=bar", "", strings.NewReader(""))
		assert.Nil(t, err)
		assert.Equal(t, "200 OK", response.Status)

		response, err = http.Post("http://localhost:3000/add?key=foo&value=foo", "", strings.NewReader(""))
		assert.Nil(t, err)
		assert.Equal(t, "200 OK", response.Status)
	}

	{
		response, err := http.Get("http://localhost:3000/get_all_local")
		assert.Nil(t, err)
		bytes, read_err := ioutil.ReadAll(response.Body)
		println(bytes)
		assert.Nil(t, read_err)
		var value http_db_server.GetAllLocalResponseBody
		json_err := json.Unmarshal(bytes, &value)
		assert.Nil(t, json_err)
		assert.Equal(t, map[string]string{"bar": "bar"}, value.Permanent_values)
		assert.Equal(t, map[string]string{}, value.Temporary_values)
	}

	{
		response, err := http.Get("http://localhost:3001/get_all_local")
		assert.Nil(t, err)
		bytes, read_err := ioutil.ReadAll(response.Body)
		assert.Nil(t, read_err)
		var value http_db_server.GetAllLocalResponseBody
		json_err := json.Unmarshal(bytes, &value)
		assert.Nil(t, json_err)
		assert.Equal(t, map[string]string{"foo": "foo"}, value.Permanent_values)
		assert.Equal(t, map[string]string{}, value.Temporary_values)
	}

}

func TestGetAllReplication2(t *testing.T) {
	server1, server2 := create_two_node_setup(2, 1, 1)

	server1.Start()
	server2.Start()

	{
		response, err := http.Post("http://localhost:3000/add?key=bar&value=bar", "", strings.NewReader(""))
		assert.Nil(t, err)
		assert.Equal(t, "200 OK", response.Status)

		response, err = http.Post("http://localhost:3000/add?key=foo&value=foo", "", strings.NewReader(""))
		assert.Nil(t, err)
		assert.Equal(t, "200 OK", response.Status)
	}

	{
		response, err := http.Get("http://localhost:3000/get_all_local")
		assert.Nil(t, err)
		bytes, read_err := ioutil.ReadAll(response.Body)
		println(bytes)
		assert.Nil(t, read_err)
		var value http_db_server.GetAllLocalResponseBody
		json_err := json.Unmarshal(bytes, &value)
		assert.Nil(t, json_err)
		assert.Equal(t, map[string]string{"bar": "bar", "foo": "foo"}, value.Permanent_values)
		assert.Equal(t, map[string]string{}, value.Temporary_values)
	}

	{
		response, err := http.Get("http://localhost:3001/get_all_local")
		assert.Nil(t, err)
		bytes, read_err := ioutil.ReadAll(response.Body)
		assert.Nil(t, read_err)
		var value http_db_server.GetAllLocalResponseBody
		json_err := json.Unmarshal(bytes, &value)
		assert.Nil(t, json_err)
		assert.Equal(t, map[string]string{"bar": "bar", "foo": "foo"}, value.Permanent_values)
		assert.Equal(t, map[string]string{}, value.Temporary_values)
	}

}
