package rrpubsub

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseUrl(t *testing.T) {
	assert := assert.New(t)

	addr, opts, err := parseUrl("redis://")
	assert.NoError(err)
	assert.Equal(len(opts), 1)
	assert.Equal(addr, "localhost:6379")

	addr, opts, err = parseUrl("")
	assert.Error(err)

	addr, opts, err = parseUrl("http://localhost")
	assert.Error(err)
	assert.Equal(err.Error(), "invalid redis URL scheme: http")

	addr, opts, err = parseUrl("redis://test:pass@localhost")
	assert.NoError(err)
	assert.Equal(len(opts), 2)
	assert.Equal(addr, "localhost:6379")

	addr, opts, err = parseUrl("redis://test:pass@localhost/1")
	assert.NoError(err)
	assert.Equal(len(opts), 3)
	assert.Equal(addr, "localhost:6379")

	addr, opts, err = parseUrl("redis://localhost/invalid")
	assert.Error(err)
	assert.Equal(err.Error(), "invalid database: invalid")
}
