package rrpubsub

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/kr/pretty"
)

type state int

const (
	disconnected state = iota
	connected
)

type eventType int

const (
	connectedEvent eventType = iota
	disconnectedEvent
)

type command struct {
	cmd  string
	args []string
}

type event struct {
	t eventType
}

const (
	redisConnectTimeout time.Duration = 5 * time.Second
	redisReadTimeout    time.Duration = 10 * time.Second
	redisWriteTimeout   time.Duration = 5 * time.Second
)

type Conn struct {
	// Received messages will be placed in this channel
	Messages chan redis.Message

	network string
	address string
	options []redis.DialOption

	lock     sync.Mutex
	channels map[string]interface{}

	commands chan command
	events   chan event

	state state

	ctx context.Context
	cf  context.CancelFunc
	wg  sync.WaitGroup

	conn    redis.Conn
	pubsub  redis.PubSubConn
	backoff time.Duration

	debug bool
}

/*
TODO:
type Dialer interface {
}
*/

// New returns a new connection that will use the given network and address with the
// specified options.
func New(ctx context.Context, network, address string, options ...redis.DialOption) *Conn {
	// Default dial options
	opts := []redis.DialOption{
		redis.DialConnectTimeout(redisConnectTimeout),
		redis.DialReadTimeout(redisReadTimeout),
		redis.DialWriteTimeout(redisWriteTimeout),
	}
	opts = append(opts, options...)

	ctx, cf := context.WithCancel(ctx)
	c := &Conn{
		Messages: make(chan redis.Message, 100),

		network:  network,
		address:  address,
		options:  opts,
		channels: make(map[string]interface{}),
		commands: make(chan command, 10),
		events:   make(chan event, 10),
		state:    disconnected,

		ctx: ctx,
		cf:  cf,

		debug: true,
	}
	c.wg.Add(1)

	c.logDebug("Starting")
	go c.run()

	// Initially disconnected
	c.logDebug("Sending disconnected")
	c.events <- event{disconnectedEvent}

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

	c.logDebug("Starting main loop")

	done := c.ctx.Done()
	for {
		select {
		case <-done:
			return

		case cmd := <-c.commands:
			switch c.state {
			case disconnected:
				// Do nothing!

			case connected:
				c.sendCommand(cmd)

			default:
				panic(fmt.Sprintf("No idea what to do with state %#v!", c.state))
			}

		case ev := <-c.events:
			pretty.Log(ev)
			switch ev.t {
			case connectedEvent:
				c.logDebug("Connected, subscribing to channels...")
				c.subscribeChannels()

			case disconnectedEvent:
				c.flushCommands()
				c.connect()
			}
		}
	}
}

func (c *Conn) sendCommand(cmd command) {
	err := c.conn.Send(cmd.cmd, sToI(cmd.args))
	if err != nil {
		panic(err)
	}

	err = c.conn.Flush()
	if err != nil {
		panic(err)
	}
}

func (c *Conn) flushCommands() {
	for {
		select {
		case <-c.commands:
		default:
			return
		}
	}
}

func (c *Conn) subscribeChannels() {
	c.lock.Lock()
	defer c.lock.Unlock()

	ch := make([]string, 0)
	for k := range c.channels {
		ch = append(ch, k)
	}

	c.commands <- command{
		cmd:  "SUBSCRIBE",
		args: ch,
	}
}

func (c *Conn) connect() {
	c.logDebug("Attempting to connect (sleeping %s)", c.backoff)
	time.Sleep(c.backoff)

	conn, err := redis.Dial(c.network, c.address, c.options...)
	if err != nil {
		c.disconnected(err)
		return
	}

	pubsub := redis.PubSubConn{Conn: conn}

	c.conn = conn
	c.pubsub = pubsub
	c.backoff = 0

	c.events <- event{connectedEvent}
}

func (c *Conn) disconnected(err error) {
	c.logDebug("Disconnected: %s", err.Error())

	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}

	c.backoff += 100 * time.Millisecond
	c.events <- event{disconnectedEvent}
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

	c.commands <- command{
		cmd:  "SUBSCRIBE",
		args: channel,
	}
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

	c.commands <- command{
		cmd:  "UNSUBSCRIBE",
		args: channel,
	}
}

func (c *Conn) logDebug(msg string, args ...interface{}) {
	if c.debug {
		log.Printf(msg, args...)
	}
}
