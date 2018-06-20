package rrpubsub

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"sync"

	"github.com/gomodule/redigo/redis"
	"github.com/kr/pretty"
)

type state int

const (
	disconnected state = iota
)

type Conn struct {
	// Received messages will be placed in this channel
	Messages chan redis.Message

	network string
	address string
	options []redis.DialOption

	lock     sync.Mutex
	channels map[string]interface{}

	commands chan []string

	state state

	ctx context.Context
	cf  context.CancelFunc
	wg  sync.WaitGroup
}

/*
TODO:
type Dialer interface {
}
*/

// New returns a new connection that will use the given network and address with the
// specified options.
func New(ctx context.Context, network, address string, options ...redis.DialOption) *Conn {
	ctx, cf := context.WithCancel(ctx)
	c := &Conn{
		Messages: make(chan redis.Message, 100),

		network:  network,
		address:  address,
		options:  options,
		channels: make(map[string]interface{}),
		commands: make(chan []string, 10),
		state:    disconnected,

		ctx: ctx,
		cf:  cf,
	}
	c.wg.Add(1)
	go c.run()
	return c
}

var pathDBRegexp = regexp.MustCompile(`/(\d*)\z`)

// NewURL returns a new connection that will connect using the Redis URI
// scheme. URLs should follow the draft IANA specification for the scheme
// (https://www.iana.org/assignments/uri-schemes/prov/redis).
func NewURL(ctx context.Context, rawurl string, options ...redis.DialOption) (*Conn, error) {
	u, err := url.Parse(rawurl)
	if err != nil {
		return nil, err
	}

	if u.Scheme != "redis" && u.Scheme != "rediss" {
		return nil, fmt.Errorf("invalid redis URL scheme: %s", u.Scheme)
	}

	// As per the IANA draft spec, the host defaults to localhost and
	// the port defaults to 6379.
	host, port, err := net.SplitHostPort(u.Host)
	if err != nil {
		// assume port is missing
		host = u.Host
		port = "6379"
	}
	if host == "" {
		host = "localhost"
	}
	address := net.JoinHostPort(host, port)

	if u.User != nil {
		password, isSet := u.User.Password()
		if isSet {
			options = append(options, redis.DialPassword(password))
		}
	}

	match := pathDBRegexp.FindStringSubmatch(u.Path)
	if len(match) == 2 {
		db := 0
		if len(match[1]) > 0 {
			db, err = strconv.Atoi(match[1])
			if err != nil {
				return nil, fmt.Errorf("invalid database: %s", u.Path[1:])
			}
		}
		if db != 0 {
			options = append(options, redis.DialDatabase(db))
		}
	} else if u.Path != "" {
		return nil, fmt.Errorf("invalid database: %s", u.Path[1:])
	}

	options = append(options, redis.DialUseTLS(u.Scheme == "rediss"))

	return New(ctx, "tcp", address, options...), nil
}

// Implements the main state machine and interacts with the underlying
// connection
func (c *Conn) run() {
	defer c.wg.Done()
	defer close(c.Messages)

	done := c.ctx.Done()
	for {
		select {
		case <-done:
			return
		case cmd := <-c.commands:
			switch c.state {
			case disconnected:
				// Do nothing!
			default:
				pretty.Log(cmd)
				panic(fmt.Sprintf("No idea what to do with state %#v!", c.state))
			}
		}
	}
}

/*
TODO
// NewConn returns a new rrpubsub connection for the given net connection.
func NewDialer(dialer Dialer, readTimeout, writeTimeout time.Duration) Conn {
	panic("Not implemented")
}
*/

// Close closes the connection.
func (c *Conn) Close() error {
	c.cf()
	c.wg.Wait()
	return nil
}

// Subscribe subscribes the connection to the specified channels.
func (c *Conn) Subscribe(channel ...string) {
	c.lock.Lock()
	defer c.lock.Unlock()

	for _, ch := range channel {
		c.channels[ch] = struct{}{}
	}

	cmd := []string{"SUBSCRIBE"}
	cmd = append(cmd, channel...)
	c.commands <- cmd
}

// Unsubscribe unsubscribes the connection from the given channels, or from all
// of them if none is given.
func (c *Conn) Unsubscribe(channel ...string) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if len(channel) == 0 {
		c.channels = make(map[string]interface{})
	} else {
		for _, ch := range channel {
			delete(c.channels, ch)
		}
	}

	cmd := []string{"UNSUBSCRIBE"}
	cmd = append(cmd, channel...)
	c.commands <- cmd
}
