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
	// 경로의 디렉터리와 이름을 기준으로 RDB, 증분 AOF, manifest를 찾는다.
	aofPath := flag.String("aof", "appendonly.aof", "persistence path for RDB and incremental AOF (empty for in-memory mode)")
	fsync := flag.String("appendfsync", "everysec", "incremental AOF fsync policy: always, everysec or no")
	primary := flag.String("replicaof", "", "follow this GoRedis primary (host:port) as a read-only replica")
	flag.Parse()

	// Ctrl+C와 SIGTERM을 context 취소로 전달한다. 서버는 이 신호를 받아
	// 리스너와 연결을 닫고 진행 중인 명령이 끝난 뒤 AOF 파일을 닫는다.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *primary != "" {
		// replica는 시작할 때 primary의 스냅샷을 받는다. 같은 디렉터리에서 두 프로세스를
		// 실행해도 replica가 primary의 기본 appendonly.aof를 열지 않도록 모드를 먼저 고른다.
		flag.Visit(func(f *flag.Flag) {
			if f.Name == "aof" && *aofPath != "" {
				log.Fatal("replica mode stores its copy in memory; omit -aof or pass -aof=''")
			}
		})
		if err := server.ListenAndServeReplica(ctx, *addr, *primary); err != nil {
			log.Fatal(err)
		}
		return
	}
	if err := server.ListenAndServeWithFsync(ctx, *addr, *aofPath, *fsync); err != nil {
		log.Fatal(err)
	}
}
