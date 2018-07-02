/*
Package rrpubsub contains a more reliable implementation of Redis Pub-Sub, backed by the redigo library.

It attempts to keep a persistent connection to the Redis server, with a retrying connection loop whenever the server connection fails.

This does not guarantee that all messages will be received: anything published while the connection was down will be lost, so it's still a best-effort thing. Just a much better effort than out of the box.
*/
package rrpubsub
