package main

import (
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func TestBalancer(t *testing.T) {
	t.Run("Heart beat passes", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()
		server1, _ := url.Parse(server.URL)

		lb := LoadBalancerInit(
			[]string{server1.Host},
			100*time.Millisecond,
			15*time.Minute,
		)

		go lb.Heartbeat()

		time.Sleep(500 * time.Millisecond)
		assert.Equal(t, true, lb.servers[0].alive)
	})

	t.Run("Heart beat fails", func(t *testing.T) {
		timeout := 50 * time.Millisecond
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(timeout + 100*time.Millisecond)
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()
		server1, _ := url.Parse(server.URL)

		lb := LoadBalancerInit(
			[]string{server1.Host},
			100*time.Millisecond,
			timeout,
		)

		go lb.Heartbeat()

		time.Sleep(150 * time.Millisecond)
		assert.Equal(t, false, lb.servers[0].alive)
	})

	t.Run("Forward request", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/health" {
				w.WriteHeader(http.StatusOK)
				return
			}
			if r.URL.Path == "/api/v1/some-data" {
				w.WriteHeader(http.StatusOK)
				_, e := w.Write([]byte("some data"))
				if e != nil {
					t.Fatal(e)
				}
				return
			}
		}))
		defer server.Close()
		server1, _ := url.Parse(server.URL)

		lb := LoadBalancerInit(
			[]string{server1.Host},
			100*time.Millisecond,
			15*time.Minute,
		)

		req := httptest.NewRequest(http.MethodGet, "/api/v1/some-data", nil)
		w := httptest.NewRecorder()

		go lb.Heartbeat()
		time.Sleep(200 * time.Millisecond)

		lb.Serve(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
		assert.NotNil(t, w.Body)
		assert.Equal(t, "some data", w.Body.String())
	})

	t.Run("Forward request with least connections", func(t *testing.T) {
		server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("server1"))
		}))
		defer server1.Close()

		server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("server2"))
		}))
		defer server2.Close()

		server1URL, _ := url.Parse(server1.URL)
		server2URL, _ := url.Parse(server2.URL)

		lb := LoadBalancerInit(
			[]string{server1URL.Host, server2URL.Host},
			100*time.Millisecond,
			15*time.Minute,
		)

		req1 := httptest.NewRequest(http.MethodGet, "/api/v1/some-data", nil)
		req2 := httptest.NewRequest(http.MethodGet, "/api/v1/some-data", nil)
		w1 := httptest.NewRecorder()
		w2 := httptest.NewRecorder()

		go lb.Heartbeat()
		time.Sleep(200 * time.Millisecond)

		go lb.Serve(w1, req1)
		go lb.Serve(w2, req2)

		time.Sleep(300 * time.Millisecond)
		bodies := []string{w1.Body.String(), w2.Body.String()}
		assert.Contains(t, bodies, "server1")
		assert.Contains(t, bodies, "server2")
	})
}

func TestLeastConnectionFunc(t *testing.T) {
	t.Run("EmptyServers", func(t *testing.T) {
		server := leastConnections([]*Server{})
		assert.Nil(t, server)
	})

	t.Run("CompareServers", func(t *testing.T) {
		servers := []*Server{{addr: "server1:8080"}, {addr: "server2:8080"}, {addr: "server3:8080"}}
		servers[0].load.Add(3)
		servers[1].load.Add(2)
		servers[2].load.Add(4)

		server := leastConnections(servers)

		assert.NotNil(t, server)
		assert.Equal(t, servers[1], server)
	})
}
