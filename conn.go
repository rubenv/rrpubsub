package rrpubsub

import (
	"context"
	"time"

	"github.com/gomodule/redigo/redis"
)

type redisConn struct {
	ctx     context.Context
	network string
	address string
	options []redis.DialOption

	conn   redis.Conn
	events chan event
}

func newRedisConn(ctx context.Context, network, address string, opts []redis.DialOption, events chan event) *redisConn {
	return &redisConn{
		ctx:     ctx,
		network: network,
		address: address,
		options: opts,
		events:  events,
	}
}

func (r *redisConn) Run() {
	conn, err := redis.Dial(r.network, r.address, r.options...)
	if err != nil {
		r.sendErr(err)
		return
	}
	defer conn.Close()

	r.conn = conn
	r.events <- event{t: connectedEvent}

	pubsub := redis.PubSubConn{Conn: conn}

	done := r.ctx.Done()
	go func() {
		for {
			select {
			case <-done:
				return
			case <-time.After(1 * time.Second):
				pubsub.Ping(time.Now().Format(time.RFC3339))
			}
		}
	}()

	for {
		select {
		case <-done:
			return
		default:
		}

		switch v := pubsub.Receive().(type) {
		case redis.Message:
			r.events <- event{
				t:   messageEvent,
				msg: v,
			}
		case error:
			r.sendErr(v)
			return
		}
	}
}

func (r *redisConn) Do(cmd command) {
	err := r.conn.Send(cmd.cmd, sToI(cmd.args)...)
	if err != nil {
		r.sendErr(err)
		return
	}

	err = r.conn.Flush()
	if err != nil {
		r.sendErr(err)
		return
	}
}

func (r *redisConn) sendErr(err error) {
	r.events <- event{
		t:   disconnectedEvent,
		c:   r,
		err: err,
	}
}

func (r *redisConn) Close() {
	if r.conn != nil {
		r.conn.Close()
		r.conn = nil
	}
}
