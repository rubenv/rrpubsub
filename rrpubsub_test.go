package rrpubsub

import (
	"context"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"
)

func TestConn(t *testing.T) {
	assert := assert.New(t)

	ctx := context.Background()

	c := New(ctx, "tcp", "localhost:6379")
	assert.NotNil(c)
	assert.NoError(c.Close())
}

func TestConnChannels(t *testing.T) {
	assert := assert.New(t)

	ctx := context.Background()
	c, err := NewURL(ctx, "redis://localhost:6379")
	assert.NoError(err)
	assert.NotNil(c)

	c.Subscribe("test")
	assert.Equal(len(c.channels), 1)

	c.Unsubscribe("test")
	assert.Equal(len(c.channels), 0)

	c.Subscribe("test", "test2", "test3")
	assert.Equal(len(c.channels), 3)

	c.Unsubscribe("test")
	assert.Equal(len(c.channels), 2)

	c.Unsubscribe()
	assert.Equal(len(c.channels), 0)

	assert.NoError(c.Close())

	_, ok := <-c.Messages
	assert.False(ok)
}

func TestConnReceive(t *testing.T) {
	assert := assert.New(t)

	s, err := newTestServer()
	assert.NoError(err)
	defer s.Kill()

	ctx := context.Background()

	c := New(ctx, "tcp", s.address)
	assert.NotNil(c)

	c.Subscribe("test")

	time.Sleep(1 * time.Second)

	assert.NoError(s.Send("test", "1"))

	msg, ok := <-c.Messages
	assert.True(ok)
	assert.NotNil(msg)

	assert.NoError(c.Close())
	_, ok = <-c.Messages
	assert.False(ok)
}

func TestConnMessages(t *testing.T) {
	assert := assert.New(t)

	s, err := newTestServer()
	assert.NoError(err)
	defer s.Kill()

	ctx := context.Background()

	c := New(ctx, "tcp", s.address)
	assert.NotNil(c)

	c.Subscribe("test")

	time.Sleep(1 * time.Second)

	s.Restart()

	c.Subscribe("test2")

	time.Sleep(1 * time.Second)

	assert.NoError(s.Send("test", "1"))

	time.Sleep(1 * time.Second)
	assert.NoError(s.Send("test2", "2"))

	time.Sleep(1 * time.Second)
	assert.NoError(c.Close())
}

func TestConnFreeze(t *testing.T) {
	assert := assert.New(t)

	s, err := newTestServer()
	assert.NoError(err)
	defer s.Kill()

	ctx := context.Background()

	c := New(ctx, "tcp", s.address, redis.DialReadTimeout(redisReadTimeout))
	assert.NotNil(c)

	s.Freeze()

	c.Subscribe("test")

	time.Sleep(3 * time.Second)

	s.Continue()

	assert.NoError(s.Send("test", "1"))

	msg, ok := <-c.Messages
	assert.True(ok)
	assert.NotNil(msg)
	assert.Equal(msg.Channel, "test")
	assert.Equal(string(msg.Data), "1")

	assert.NoError(c.Close())
	_, ok = <-c.Messages
	assert.False(ok)
}
