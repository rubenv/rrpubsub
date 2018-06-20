package rrpubsub

import (
	"testing"

	"github.com/cheekybits/is"
)

func TestParseUrl(t *testing.T) {
	is := is.New(t)

	addr, opts, err := parseUrl("redis://")
	is.NoErr(err)
	is.Equal(len(opts), 1)
	is.Equal(addr, "localhost:6379")

	addr, opts, err = parseUrl("")
	is.Err(err)

	addr, opts, err = parseUrl("http://localhost")
	is.Err(err)
	is.Equal(err.Error(), "invalid redis URL scheme: http")

	addr, opts, err = parseUrl("redis://test:pass@localhost")
	is.NoErr(err)
	is.Equal(len(opts), 2)
	is.Equal(addr, "localhost:6379")

	addr, opts, err = parseUrl("redis://test:pass@localhost/1")
	is.NoErr(err)
	is.Equal(len(opts), 3)
	is.Equal(addr, "localhost:6379")

	addr, opts, err = parseUrl("redis://localhost/invalid")
	is.Err(err)
	is.Equal(err.Error(), "invalid database: invalid")
}
