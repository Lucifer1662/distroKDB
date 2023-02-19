package distributed_hash_ring

import (
	"log"
	"net"
	"net/rpc"
	"strconv"

	"github.com/lucifer1662/distrokdb/node/hash_ring"
)

type DistributedTable struct {
	server_address string
	position       hash_ring.KeyHash
}

func (t *DistributedTable) Add(key string, value string) error {
	client, err := rpc.Dial("tcp", t.server_address)
	if err != nil {
		return err
	}

	// Synchronous call
	args := &AddRequest{key, value, t.position}
	var reply AddResponse

	//blocks for response
	err = client.Call("DistributedHashRingServer.Add", args, &reply)
	if err != nil {
		return err
	}
	return nil
}

func (t *DistributedTable) Get(key string) (*string, error) {
	client, err := rpc.Dial("tcp", t.server_address)
	if err != nil {
		return nil, err
	}

	// Synchronous call
	args := &GetRequest{key, t.position}
	var reply GetResponse

	//blocks for response
	err = client.Call("DistributedHashRingServer.Get", args, &reply)
	if err != nil {
		return nil, err
	}
	return reply.Value, nil
}

func (t *DistributedTable) Remove(key string) error {
	return nil

}

func (t *DistributedTable) Size() int {
	return 0
}

func (t *DistributedTable) Iter() hash_ring.KeyValueIterator {
	panic("Iterating over distributed table is not supported, and operation should be moved onto the node directly")
}

func (t *DistributedTable) Erase(key string) {
	panic("Erasing over distributed table is not supported, and operation should be moved onto the node directly")
}

type DistributedHashRingServer struct {
	hash_ring  *hash_ring.Hash_Ring
	rpc_server *rpc.Server
	listener   *net.Listener
	port       int
}

func NewServer(hr *hash_ring.Hash_Ring, port int) *DistributedHashRingServer {
	rpc_server := rpc.NewServer()
	s := DistributedHashRingServer{hr, rpc_server, nil, port}
	rpc_server.Register(&s)
	return &s
}

type AddRequest struct {
	Key           string
	Value         string
	Node_position hash_ring.KeyHash
}

type AddResponse struct {
	Success       bool
	Error_message string
}

func (t *DistributedHashRingServer) Add(request AddRequest, response *AddResponse) error {
	var err error
	err = t.hash_ring.AddToNodePermanent(request.Node_position, request.Key, request.Value)

	response.Success = err == nil
	if !response.Success {
		response.Error_message = err.Error()
	}
	return err
}

type GetRequest struct {
	Key           string
	Node_position hash_ring.KeyHash
}

type GetResponse struct {
	Success       bool
	Value         *string
	Error_message string
}

func (t *DistributedHashRingServer) Get(request GetRequest, response *GetResponse) error {
	var err error
	var value *string
	value, err = t.hash_ring.GetFromNodePermanent(request.Node_position, request.Key)

	response.Success = err == nil
	if !response.Success {
		response.Error_message = err.Error()
	}
	response.Value = value
	return err
}

func (server *DistributedHashRingServer) Start() {
	listener, e := net.Listen("tcp", ":"+strconv.Itoa(server.port))
	server.listener = &listener
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go server.rpc_server.Accept(*server.listener)
}

func (server *DistributedHashRingServer) Stop() {
	if server.listener != nil {
		(*server.listener).Close()
	}
}
