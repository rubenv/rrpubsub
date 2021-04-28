package rrpubsub

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
)

type state int

const (
	initial state = iota
	disconnected
	connected
)

type eventType int

const (
	connectedEvent eventType = iota
	disconnectedEvent
	messageEvent
)

type command struct {
	cmd  string
	args []string
}

type event struct {
	t   eventType
	c   *redisConn
	msg redis.Message
	err error
}

const (
	redisPingInterval   time.Duration = 1 * time.Second
	redisConnectTimeout time.Duration = 5 * time.Second
	redisReadTimeout    time.Duration = 10 * time.Second
	redisWriteTimeout   time.Duration = 5 * time.Second
)

type logger func(msg string)

type Conn interface {
	Messages() <-chan redis.Message
	Subscribe(channel ...string)
	Unsubscribe(channel ...string)
	Close() error
}

type conn struct {
	// Received messages will be placed in this channel
	messages chan redis.Message

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

	conn    *redisConn
	backoff time.Duration

	debug logger
}

// New returns a new connection that will use the given network and address with the
// specified options.
func New(ctx context.Context, network, address string, options ...redis.DialOption) Conn {
	// Default dial options
	opts := []redis.DialOption{
		redis.DialConnectTimeout(redisConnectTimeout),
		redis.DialReadTimeout(redisReadTimeout),
		redis.DialWriteTimeout(redisWriteTimeout),
	}
	opts = append(opts, options...)

	ctx, cf := context.WithCancel(ctx)
	c := &conn{
		messages: make(chan redis.Message, 100),

		network:  network,
		address:  address,
		options:  opts,
		channels: make(map[string]interface{}),
		commands: make(chan command, 10),
		events:   make(chan event, 10),
		state:    initial,

		ctx: ctx,
		cf:  cf,
	}
	c.wg.Add(1)

	c.logDebug("Starting")
	go c.run()

	// Initially disconnected
	c.logDebug("Sending disconnected")
	c.events <- event{t: disconnectedEvent}

	return c
}

func (c *conn) Messages() <-chan redis.Message {
	return c.messages
}

// Implements the main state machine and interacts with the underlying
// connection
func (c *conn) run() {
	defer c.wg.Done()
	defer close(c.messages)

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
				c.logDebug("CMD: %s, %#v", cmd.cmd, cmd.args)
				c.conn.Do(cmd)
			}

		case ev := <-c.events:
			switch ev.t {
			case connectedEvent:
				c.logDebug("Connected, subscribing to channels...")
				c.state = connected
				c.backoff = 0
				c.subscribeChannels()

			case disconnectedEvent:
				if ev.c == c.conn {
					if ev.err != nil {
						c.logDebug("Disconnected: %s", ev.err.Error())
					}
					c.state = disconnected
					c.flushCommands()
					c.closeConnection()
					c.connect()
				}

			case messageEvent:
				c.messages <- ev.msg
			}
		}
	}
}

func (c *conn) flushCommands() {
	for {
		select {
		case <-c.commands:
		default:
			return
		}
	}
}

func (c *conn) subscribeChannels() {
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

func (c *conn) closeConnection() {
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
}

func (c *conn) connect() {
	if c.state == connected {
		return
	}

	c.logDebug("Attempting to connect (sleeping %s)", c.backoff)

	time.Sleep(c.backoff)
	if c.backoff < 1*time.Second {
		c.backoff += 100 * time.Millisecond
	}

	c.conn = newRedisConn(c.ctx, c.network, c.address, c.options, c.events)
	go c.conn.Run()
}

// Close closes the connection.
func (c *conn) Close() error {
	c.cf()
	c.wg.Wait()
	return nil
}

// Subscribe subscribes the connection to the specified channels.
func (c *conn) Subscribe(channel ...string) {
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
func (c *conn) Unsubscribe(channel ...string) {
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

func (c *conn) logDebug(msg string, args ...interface{}) {
	if c.debug != nil {
		c.debug(fmt.Sprintf(msg, args...))
	}
}
