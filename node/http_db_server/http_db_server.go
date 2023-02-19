package http_db_server

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/lucifer1662/distrokdb/node/hash_ring"
)

type HttpDBServer struct {
	hr                   *hash_ring.Hash_Ring
	http_external_server *http.Server
	My_id                uint64
}

type Config struct {
	Http_port int
	My_id     uint64
}

func NewHttpDBServer(config *Config, hr *hash_ring.Hash_Ring) *HttpDBServer {
	http_mux := http.NewServeMux()

	http_external_server := http.Server{
		Addr:        ":" + strconv.Itoa(config.Http_port),
		ConnContext: SaveConnInContext,
		Handler:     http_mux,
	}

	db := HttpDBServer{
		hr,
		&http_external_server,
		config.My_id,
	}

	http_mux.HandleFunc("/add", db.add)
	http_mux.HandleFunc("/get", db.get)
	http_mux.HandleFunc("/get_all_local", db.get_all_local)

	return &db

}

func (db *HttpDBServer) Stop() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.http_external_server.Shutdown(ctx); err != nil {
		// handle err
	}
}

func (db *HttpDBServer) Start() {
	db.http_external_server.ListenAndServe()
}

func (db *HttpDBServer) get(w http.ResponseWriter, req *http.Request) {
	query := req.URL.Query()

	if !query.Has("key") {
		w.WriteHeader(400)
		return
	}

	key := query.Get("key")

	value, err := db.hr.Get(key)

	if err == nil {
		json_string, json_err := json.Marshal(value)
		if json_err == nil {
			w.WriteHeader(200)
			w.Write(json_string)
		} else {
			w.WriteHeader(500)
		}
	} else {
		w.WriteHeader(500)
	}
}

func (db *HttpDBServer) add(w http.ResponseWriter, req *http.Request) {
	query := req.URL.Query()

	if !query.Has("key") {
		w.WriteHeader(400)
		return
	}

	key := query.Get("key")

	var value string = ""
	if query.Has("value") {
		value = query.Get("value")
	}

	err := db.hr.Add(key, value)

	if err == nil {
		w.WriteHeader(200)
	} else {
		w.WriteHeader(500)
	}
}

type GetAllLocalResponseBody struct {
	Permanent_values map[string]string `json:"permanent_values"`
	Temporary_values map[string]string `json:"temporary_values"`
}

func (db *HttpDBServer) get_all_local(w http.ResponseWriter, req *http.Request) {
	nodes := db.hr.Nodes()
	perm_values := make(map[string]string)
	temp_values := make(map[string]string)

	for i := range nodes {
		if db.My_id == nodes[i].GetPhysicalId() {
			hash_ring.CopyToMap(nodes[i].GetTable(), &perm_values)
			hash_ring.CopyToMap(nodes[i].GetTemporaryTable(), &temp_values)
		}
	}

	structured_data := GetAllLocalResponseBody{
		perm_values,
		temp_values,
	}
	json_string, json_err := json.Marshal(structured_data)

	if json_err == nil {
		w.WriteHeader(200)
		w.Write(json_string)
	} else {
		w.WriteHeader(500)
	}
}

func headers(w http.ResponseWriter, req *http.Request) {
	for name, headers := range req.Header {
		for _, h := range headers {
			fmt.Fprintf(w, "%v: %v\n", name, h)
		}
	}
}

func SaveConnInContext(ctx context.Context, c net.Conn) context.Context {
	return context.WithValue(ctx, "http-conn", c)
}
func GetConn(r *http.Request) net.Conn {
	return r.Context().Value("http-conn").(net.Conn)
}
