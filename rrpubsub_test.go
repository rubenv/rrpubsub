package rrpubsub

import (
	"testing"

	"github.com/cheekybits/is"
)

func TestConn(t *testing.T) {
	is := is.New(t)

	c := New("tcp", "localhost:6379")
	is.NotNil(c)

	c, err := NewURL("redis://localhost:6379")
	is.NoErr(err)
	is.NotNil(c)
}
