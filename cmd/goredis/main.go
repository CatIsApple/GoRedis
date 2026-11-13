package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/CatIsApple/GoRedis/internal/server"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:6379", "TCP listen address")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := server.ListenAndServe(ctx, *addr); err != nil {
		log.Fatal(err)
	}
}
