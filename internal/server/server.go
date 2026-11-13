package server

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"sync"

	"github.com/CatIsApple/GoRedis/internal/resp"
)

// ListenAndServe는 addr에 TCP 리스너를 열고 클라이언트 연결을 받는다.
func ListenAndServe(ctx context.Context, addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return Serve(ctx, listener)
}

// Serve는 주어진 리스너를 사용해 메모리 전용 서버를 실행한다.
// 리스너의 소유권은 서버로 넘어가며 종료 시 열린 연결과 함께 닫힌다.
func Serve(ctx context.Context, listener net.Listener) error {
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Go(func() {
		<-ctx.Done()
		listener.Close() // Accept의 대기를 해제한다.
	})
	defer func() {
		cancel()
		wg.Wait()
	}()

	log.Printf("GoRedis listening on %s", listener.Addr())
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		// Accept 루프는 이 연결의 요청을 기다리지 않고 다음 접속을 받는다.
		wg.Go(func() { handleConn(ctx, conn) })
	}
}

func handleConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	// 서버 종료 때 소켓을 닫으면 이 연결에서 대기 중인 읽기도 깨어난다.
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()

	// Reader가 다음 요청의 바이트까지 미리 읽을 수 있어 연결 내에서 계속 재사용한다.
	reader := resp.NewReader(conn)
	writer := resp.NewWriter(conn)
	for {
		_, err := reader.ReadValue()
		if err != nil {
			if errors.Is(err, resp.ErrProtocol) || errors.Is(err, io.ErrUnexpectedEOF) {
				writeReply(writer, resp.Value{Kind: resp.SimpleError, Bytes: []byte("ERR Protocol error")})
			}
			return
		}
		// 다음 단계에서 단일 실행기에 요청을 전달한다.
		reply := resp.Value{Kind: resp.SimpleError, Bytes: []byte("ERR command execution not implemented")}
		if writeReply(writer, reply) != nil {
			return
		}
	}
}

func writeReply(writer *resp.Writer, reply resp.Value) error {
	if err := writer.WriteValue(reply); err != nil {
		return err
	}
	return writer.Flush()
}
