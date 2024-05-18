package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/roman-mazur/architecture-practice-4-template/httptools"
	"github.com/roman-mazur/architecture-practice-4-template/signal"
	"io"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

var (
	port       = flag.Int("port", 8090, "load balancer port")
	timeoutSec = flag.Int("timeout-sec", 3, "request timeout time in seconds")
	https      = flag.Bool("https", false, "whether backends support HTTPs")

	traceEnabled = flag.Bool("trace", false, "whether to include tracing information into responses")
)

type Server struct {
	addr    string
	alive   bool
	load    atomic.Int32
	timeout time.Duration
	secured bool
}

func (s *Server) Scheme() string {
	if s.secured {
		return "https"
	}
	return "http"
}

func (s *Server) CheckHealth() {
	ctx, _ := context.WithTimeout(context.Background(), s.timeout)
	req, _ := http.NewRequestWithContext(ctx, "GET",
		fmt.Sprintf("%s://%s/health", s.Scheme(), s.addr), nil)
	resp, err := http.DefaultClient.Do(req)

	if err != nil || resp.StatusCode != http.StatusOK {
		s.alive = false
	} else {
		s.alive = true
	}
}

type LoadBalancer struct {
	servers        []*Server
	pickServerLock sync.Mutex
	heartbeat      time.Duration
	timeout        time.Duration
	pickMethod     func([]*Server) *Server
}

func LoadBalancerInit(servers []string, heartbeat time.Duration, timeout time.Duration) *LoadBalancer {
	var srvs []*Server
	for _, s := range servers {
		srvs = append(srvs, &Server{addr: s, timeout: timeout, secured: *https})
	}
	return &LoadBalancer{
		servers:    srvs,
		heartbeat:  heartbeat,
		timeout:    timeout,
		pickMethod: leastConnections,
	}
}

func (lb *LoadBalancer) syncPickServer() *Server {
	lb.pickServerLock.Lock()
	defer lb.pickServerLock.Unlock()
	server := lb.pickMethod(lb.aliveServers())
	server.load.Add(1)
	return server
}

func (lb *LoadBalancer) forward(rw http.ResponseWriter, r *http.Request) error {
	dst := lb.syncPickServer()
	defer dst.load.Add(-1)

	if dst == nil {
		rw.WriteHeader(http.StatusServiceUnavailable)
		return fmt.Errorf("no alive servers")
	}

	ctx, _ := context.WithTimeout(r.Context(), lb.timeout)
	fwdRequest := r.Clone(ctx)
	fwdRequest.RequestURI = ""
	fwdRequest.URL.Host = dst.addr
	fwdRequest.URL.Scheme = dst.Scheme()
	fwdRequest.Host = dst.addr

	resp, err := http.DefaultClient.Do(fwdRequest)
	if err == nil {
		for k, values := range resp.Header {
			for _, value := range values {
				rw.Header().Add(k, value)
			}
		}
		if *traceEnabled {
			rw.Header().Set("lb-from", dst.addr)
		}
		log.Println("fwd", resp.StatusCode, resp.Request.URL)
		rw.WriteHeader(resp.StatusCode)
		defer resp.Body.Close()
		_, err := io.Copy(rw, resp.Body)
		if err != nil {
			log.Printf("Failed to write response: %s", err)
		}
		return nil
	} else {
		log.Printf("Failed to get response from %s: %s", dst.addr, err)
		rw.WriteHeader(http.StatusServiceUnavailable)
		return err
	}
}

func (lb *LoadBalancer) aliveServers() []*Server {
	var alive []*Server
	for _, s := range lb.servers {
		if s.alive {
			alive = append(alive, s)
		}
	}
	return alive
}

func (lb *LoadBalancer) Serve(rw http.ResponseWriter, r *http.Request) {
	err := lb.forward(rw, r)
	if err != nil {
		rw.WriteHeader(http.StatusServiceUnavailable)
		log.Printf("Failed to process request: %s", err)
	}
}

func (lb *LoadBalancer) Heartbeat() {
	for range time.Tick(lb.heartbeat) {
		for _, s := range lb.servers {
			s.CheckHealth()
		}
	}
}

func leastConnections(servers []*Server) *Server {
	if len(servers) == 0 {
		return nil
	}
	least := servers[0]
	for _, s := range servers {
		if s.load.Load() < least.load.Load() {
			least = s
		}
	}
	return least
}

func main() {
	flag.Parse()
	lb := LoadBalancerInit(
		[]string{"server1:8080", "server2:8080", "server3:8080"},
		3*time.Second,
		time.Duration(*timeoutSec)*time.Second,
	)

	go lb.Heartbeat()

	frontend := httptools.CreateServer(*port, http.HandlerFunc(lb.Serve))

	log.Println("Starting load balancer...")
	log.Printf("Tracing support enabled: %t", *traceEnabled)
	frontend.Start()
	signal.WaitForTerminationSignal()
}
