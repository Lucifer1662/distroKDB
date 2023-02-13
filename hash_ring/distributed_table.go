package hash_ring

import (
	"log"
	"net"
	"net/rpc"
	"strconv"
)

type LocalTable struct {
	table KeyValueTable
}

func (t *LocalTable) Add(key string, value string) error {
	return t.table.Add(key, value)
}

func (t *LocalTable) Get(key string) (*string, error) {
	return t.table.Get(key)
}

func (t *LocalTable) Remove(key string) error {
	return t.table.Remove(key)

}

func (t *LocalTable) Size() int {
	return t.table.Size()
}

type DistributedTable struct {
	server_address string
}

func (t *DistributedTable) Add(key string, value string) error {
	client, err := rpc.Dial("tcp", t.server_address)
	if err != nil {
		return err
	}

	// Synchronous call
	args := &AddRequest{key, value}
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
	args := &GetRequest{key}
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

type DistributedHashRingServer struct {
	hash_ring *Hash_Ring
}

type AddRequest struct {
	Key   string
	Value string
}

type AddResponse struct {
	Success bool
}

func (t *DistributedHashRingServer) Add(request AddRequest, response *AddResponse) error {
	t.hash_ring.Add(request.Key, request.Value)
	response.Success = true
	return nil
}

type GetRequest struct {
	Key string
}

type GetResponse struct {
	Success bool
	Value   *string
}

func (t *DistributedHashRingServer) Get(request GetRequest, response *GetResponse) error {
	value, err := t.hash_ring.Get(request.Key)
	response.Success = err != nil
	response.Value = value
	return err
}

func (handler *DistributedHashRingServer) Start(port int) {
	server := rpc.NewServer()
	server.Register(handler)
	l, e := net.Listen("tcp", ":"+strconv.Itoa(port))
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go server.Accept(l)
	// rpc.Register(handler)
	// rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":"+strconv.Itoa(port))
	// if e != nil {
	// 	log.Fatal("listen error:", e)
	// }
	// go http.Serve(l, nil)
}
