package rrpubsub

import (
	"context"
	"testing"
	"time"

	"github.com/cheekybits/is"
)

func TestConn(t *testing.T) {
	is := is.New(t)

	ctx := context.Background()

	c := New(ctx, "tcp", "localhost:6379")
	is.NotNil(c)
	is.NoErr(c.Close())
}

func TestConnChannels(t *testing.T) {
	is := is.New(t)

	ctx := context.Background()
	c, err := NewURL(ctx, "redis://localhost:6379")
	is.NoErr(err)
	is.NotNil(c)

	c.Subscribe("test")
	is.Equal(len(c.channels), 1)

	c.Unsubscribe("test")
	is.Equal(len(c.channels), 0)

	c.Subscribe("test", "test2", "test3")
	is.Equal(len(c.channels), 3)

	c.Unsubscribe("test")
	is.Equal(len(c.channels), 2)

	c.Unsubscribe()
	is.Equal(len(c.channels), 0)

	is.NoErr(c.Close())

	_, ok := <-c.Messages
	is.False(ok)
}

func TestConnReceive(t *testing.T) {
	is := is.New(t)

	s, err := newTestServer()
	is.NoErr(err)
	defer s.Kill()

	ctx := context.Background()

	c := New(ctx, "tcp", s.address)
	is.NotNil(c)

	c.Subscribe("test")

	time.Sleep(1 * time.Second)

	is.NoErr(s.Send("test", "1"))

	msg, ok := <-c.Messages
	is.True(ok)
	is.NotNil(msg)

	is.NoErr(c.Close())
	_, ok = <-c.Messages
	is.False(ok)
}

func TestConnMessages(t *testing.T) {
	is := is.New(t)

	s, err := newTestServer()
	is.NoErr(err)
	defer s.Kill()

	ctx := context.Background()

	c := New(ctx, "tcp", s.address)
	is.NotNil(c)

	c.Subscribe("test")

	time.Sleep(1 * time.Second)

	s.Restart()

	c.Subscribe("test2")

	time.Sleep(1 * time.Second)

	is.NoErr(s.Send("test", "1"))

	time.Sleep(1 * time.Second)
	is.NoErr(s.Send("test2", "2"))

	time.Sleep(1 * time.Second)
	is.NoErr(c.Close())
}
