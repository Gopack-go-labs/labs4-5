package integration

import (
	"fmt"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"
)

const baseAddress = "http://balancer:8090"

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
		requestCount++

		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))

			if err != nil {
				t.Logf("Could not get respone from server")
				return
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
}

func BenchmarkBalancer(b *testing.B) {
	var wg sync.WaitGroup
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		b.Skip("Integration benchmark is not enabled")
	}

	reqCount := 100
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
