package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/roman-mazur/architecture-practice-4-template/httptools"
	"github.com/roman-mazur/architecture-practice-4-template/signal"
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

var port = flag.Int("port", 8080, "server port")

const teamName = "gopack"
const dbUrl = "http://db:8083/db"
const confResponseDelaySec = "CONF_RESPONSE_DELAY_SEC"
const confHealthFailure = "CONF_HEALTH_FAILURE"

func main() {
  client := http.DefaultClient
  h := new(http.ServeMux)
  
  h.HandleFunc("/health", func(rw http.ResponseWriter, r *http.Request) {
    rw.Header().Set("content-type", "text/plain")
    if failConfig := os.Getenv(confHealthFailure); failConfig == "true" {
      rw.WriteHeader(http.StatusInternalServerError)
      _, _ = rw.Write([]byte("FAILURE"))
    } else {
      rw.WriteHeader(http.StatusOK)
      _, _ = rw.Write([]byte("OK"))
    }
  })

  report := make(Report)

  h.HandleFunc("/api/v1/some-data", func(rw http.ResponseWriter, r *http.Request) {
    respDelayString := os.Getenv(confResponseDelaySec)
    if delaySec, parseErr := strconv.Atoi(respDelayString); parseErr == nil && delaySec > 0 && delaySec < 300 {
      time.Sleep(time.Duration(delaySec) * time.Second)
    }

    report.Process(r)

	key := r.URL.Query().Get("key")
	if key == "" {
		rw.Header().Set("content-type", "application/json")
		rw.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(rw).Encode([]string{
			"Try using 'key' query param",
		})
		return
	}

	resp, err := client.Get(fmt.Sprintf("%s/%s", dbUrl, key))
	if err != nil {
		rw.WriteHeader(http.StatusNotFound)
		return
	}
	defer resp.Body.Close()

	var body Res
	if resp.StatusCode == http.StatusNotFound {
		rw.WriteHeader(http.StatusNotFound)
		return
	}
	
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}

	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(rw).Encode(body)
  })

  h.Handle("/report", report)

  server := httptools.CreateServer(*port, h)
  server.Start()

  buffer := new(bytes.Buffer)
  body := Req{Value: time.Now().Format(time.RFC3339), Type: "string"}
  if err := json.NewEncoder(buffer).Encode(body); err != nil {
    fmt.Println("Failed to encode request body:", err)
    return
  }

  res, err := client.Post(fmt.Sprintf("%s/%s", dbUrl, teamName), "application/json", buffer)
  if err != nil {
    fmt.Println("Failed to send POST request:", err)
    return
  }
  defer res.Body.Close()

  signal.WaitForTerminationSignal()
}
