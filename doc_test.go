package rrpubsub

import (
	"context"
	"fmt"
)

func ExampleConn() {
	ctx := context.Background()
	conn := New(ctx, "tcp", "localhost:6379")
	conn.Subscribe("mychannel")

	messages := conn.Messages()

	for {
		select {
		case msg, ok := <-messages:
			if !ok {
				break
			}

			fmt.Printf("%#v", msg)
		}
	}
}
