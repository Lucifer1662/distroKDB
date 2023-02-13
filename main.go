package main

import (
	"context"
	"fmt"
	"luke/distrokdb/hash_ring"
	"net"
	"net/http"
)

func GetEvents(w http.ResponseWriter, req *http.Request) {
	hr := hash_ring.Hash_Ring{nodes: hash_ring.Generate_Nodes(5)}
	hr.Add("foo", "bar")
	// query := req.URL.Query()

	// topic_name := query.Get("topic")
	// event_id, _ := strconv.Atoi(query.Get("event_id"))
	// buffer_size, _ := strconv.Atoi(query.Get("buffer_size"))

}

func AddEvents(w http.ResponseWriter, req *http.Request) {
	// query := req.URL.Query()

	// topic_name := query.Get("topic")

	w.WriteHeader(200)
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

func main() {
	http.HandleFunc("/getEvents", GetEvents)
	http.HandleFunc("/addEvents", AddEvents)
	http.HandleFunc("/headers", headers)

	server := http.Server{
		Addr:        ":3000",
		ConnContext: SaveConnInContext,
	}
	server.ListenAndServe()

}
