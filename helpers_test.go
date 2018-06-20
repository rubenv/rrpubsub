package rrpubsub

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"os/exec"
	"time"

	"github.com/gomodule/redigo/redis"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func logDebug(msg string) {
	log.Println(msg)
}

type testServer struct {
	port    int
	address string
	cmd     *exec.Cmd
}

func newTestServer() (*testServer, error) {
	port := 6000 + rand.Intn(5000)

	s := &testServer{
		port:    port,
		address: fmt.Sprintf("localhost:%d", port),
	}
	s.start()

	return s, nil
}

func (s *testServer) Kill() {
	if s.cmd != nil {
		s.cmd.Process.Kill()
		s.cmd.Wait()
		s.cmd = nil
	}
}

func (s *testServer) start() {
	s.cmd = exec.Command("redis-server", "--port", fmt.Sprintf("%d", s.port))
	s.cmd.Start()

	// Wait for it to be up
	for {
		conn, err := net.Dial("tcp", s.address)
		if err == nil {
			conn.Close()
			break
		}
	}
}

func (s *testServer) Restart() {
	s.Kill()
	s.start()
}

func (s *testServer) Send(ch, msg string) error {
	c, err := redis.Dial("tcp", s.address)
	if err != nil {
		return err
	}

	//	log.Printf("PUBLISH: %s %s", ch, msg)
	_, err = c.Do("PUBLISH", ch, msg)
	return err
}
