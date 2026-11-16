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
	// 상대 경로는 서버를 실행한 디렉터리 기준이다. 재시작할 때 같은 파일을
	// 지정하면 이전 명령을 재생해 키와 만료 시각을 되살릴 수 있다.
	aofPath := flag.String("aof", "appendonly.aof", "append-only file path (empty for in-memory mode)")
	flag.Parse()

	// Ctrl+C와 SIGTERM을 context 취소로 전달한다. 서버는 이 신호를 받아
	// 리스너와 연결을 닫고 진행 중인 명령이 끝난 뒤 AOF 파일을 닫는다.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := server.ListenAndServe(ctx, *addr, *aofPath); err != nil {
		log.Fatal(err)
	}
}
