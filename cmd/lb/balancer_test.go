package integration

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"
)

type Res struct {
  Key   string `json:"key"`
  Value string `json:"value"`
  Type  string `json:"type"`
}

const baseAddress = "http://balancer:8090"
const teamName = "gopack"

var client = http.Client{
  Timeout: 3 * time.Second,
}

func TestBalancer(t *testing.T) {
  var wg sync.WaitGroup
  var mu sync.Mutex
  if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
    t.Skip("Integration test is not enabled")
  }

  serverHits := make(map[string]int)
  requestCount := 100

  for i := 0; i < requestCount; i++ {
    wg.Add(1)
    go func() {
      defer wg.Done()
      var body Res
      
      resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data?key=%s", baseAddress, teamName))
      if err != nil {
        t.Error("Could not get response from server")
        return
      }

      err = json.NewDecoder(resp.Body).Decode(&body)
      if err != nil {
        t.Error("Cannot decode response body")
      }

      if body.Key != teamName || body.Type != "string" {
        t.Error("Response body contains wrong information")
      }

      server := resp.Header.Get("lb-from")
      mu.Lock()
      serverHits[server]++
      mu.Unlock()
      resp.Body.Close()
    }()

  }
  wg.Wait()

  for server, hits := range serverHits {
    t.Logf("Server %s handled %d requests", server, hits)

  }

  // Validate that the requests were distributed to more than one server
  if len(serverHits) < 2 {
    t.Error("Load was not distributed to multiple servers")
  }

  resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data?key=asdf", baseAddress))
  if err != nil {
    t.Error("Could not get response from server")
    return
  }

  if resp.StatusCode != http.StatusNotFound {
    t.Error("Wrong status code for non existing key")
  }

  if bodyBytes, _ := io.ReadAll(resp.Body); len(bodyBytes) != 0 {
    t.Error("Body of the 404 response is not empty")
  }

  resp.Body.Close()
}

func BenchmarkBalancer(b *testing.B) {
  var wg sync.WaitGroup
  if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
    b.Skip("Integration benchmark is not enabled")
  }

  reqCount := 1000
  start := time.Now()

  for n := 0; n < reqCount; n++ {
    wg.Add(1)
    go func() {
      defer wg.Done()
      resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))
      if err != nil {
        b.Error(err)
        return
      }
      resp.Body.Close()
    }()
  }
  wg.Wait()

  t := time.Now()
  elapsed := t.Sub(start)
  b.Logf("Processed %d requests in %v", reqCount, elapsed)
}
