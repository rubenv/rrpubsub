package rrpubsub

import (
	"context"
	"fmt"
)

func ExampleConn() {
	ctx := context.Background()
	conn := New(ctx, "tcp", "localhost:6379")
	conn.Subscribe("mychannel")

	for {
		select {
		case msg, ok := <-conn.Messages:
			if !ok {
				break
			}

			fmt.Printf("%#v", msg)
		}
	}
}
