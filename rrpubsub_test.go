package rrpubsub

import (
	"context"
	"testing"

	"github.com/cheekybits/is"
)

func TestConn(t *testing.T) {
	is := is.New(t)

	ctx := context.Background()

	c := New(ctx, "tcp", "localhost:6379")
	is.NotNil(c)
	is.NoErr(c.Close())

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
