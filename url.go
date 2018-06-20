package rrpubsub

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"strconv"

	"github.com/gomodule/redigo/redis"
)

var pathDBRegexp = regexp.MustCompile(`/(\d*)\z`)

// NewURL returns a new connection that will connect using the Redis URI
// scheme. URLs should follow the draft IANA specification for the scheme
// (https://www.iana.org/assignments/uri-schemes/prov/redis).
func NewURL(ctx context.Context, rawurl string, options ...redis.DialOption) (*Conn, error) {
	address, opts, err := parseUrl(rawurl)
	if err != nil {
		return nil, err
	}
	options = append(options, opts...)
	return New(ctx, "tcp", address, options...), nil
}

func parseUrl(rawurl string) (string, []redis.DialOption, error) {
	options := make([]redis.DialOption, 0)

	u, err := url.Parse(rawurl)
	if err != nil {
		return "", nil, err
	}

	if u.Scheme != "redis" && u.Scheme != "rediss" {
		return "", nil, fmt.Errorf("invalid redis URL scheme: %s", u.Scheme)
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
				return "", nil, fmt.Errorf("invalid database: %s", u.Path[1:])
			}
		}
		if db != 0 {
			options = append(options, redis.DialDatabase(db))
		}
	} else if u.Path != "" {
		return "", nil, fmt.Errorf("invalid database: %s", u.Path[1:])
	}

	options = append(options, redis.DialUseTLS(u.Scheme == "rediss"))
	return address, options, nil
}
