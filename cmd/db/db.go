package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"

	"github.com/Gopack-go-labs/labs4-5/datastore"
	"github.com/Gopack-go-labs/labs4-5/httptools"
	"github.com/Gopack-go-labs/labs4-5/signal"
	"github.com/gorilla/mux"
)

type Res struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	Type  string `json:"type"`
}

type Req struct {
	Value string `json:"value"`
	Type  string `json:"type"`
}

func main() {
	httpHandler := mux.NewRouter()

	dir, err := ioutil.TempDir("", "temp-dir")
	if err != nil {
		log.Fatal(err)
	}

	db, err := datastore.NewDb(dir, 10*datastore.Megabyte)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Close()
	
	httpHandler.HandleFunc("/db/{key}", func(rw http.ResponseWriter, req *http.Request) {
		urlStr := req.URL.String()
		myUrl, _ := url.Parse(urlStr)
		params, _ := url.ParseQuery(myUrl.RawQuery)
		
		vars := mux.Vars(req)
		key := vars["key"]
	
		switch req.Method {
			
		case http.MethodGet:
			var val string
			var err error
	
			if params.Get("type") == "int64" {
				var data int64
				data, err = db.GetInt64(key)
				val = strconv.FormatInt(data, 10)
			} else {
				val, err = db.GetString(key)
			}

			if err != nil {
				rw.WriteHeader(404)
				return
			}
			
			rw.Header().Set("Content-Type", "application/json")
			rw.WriteHeader(http.StatusOK)
			rw.Write([]byte(val))

		case http.MethodPost:
			var body Req

			// TODO: Allow passing integers to json object
			err := json.NewDecoder(req.Body).Decode(&body)
			if err != nil {
				rw.WriteHeader(http.StatusBadRequest)
				return
			}

			val, err := strconv.ParseInt(body.Value, 10, 64)

			if err != nil {
				err = db.PutString(key, body.Value)
				fmt.Println("Post string")
			} else {
				err = db.PutInt64(key, val)
				fmt.Println("Post int64")
			}

			if err != nil {
				rw.WriteHeader(http.StatusInternalServerError)
				return
			}
			
			rw.WriteHeader(http.StatusCreated)
		}
	})

	server := httptools.CreateServer(8080, httpHandler)

	server.Start()

	signal.WaitForTerminationSignal()
}