package server

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"net"
	"sync"

	"github.com/CatIsApple/GoRedis/internal/engine"
	"github.com/CatIsApple/GoRedis/internal/persist"
	"github.com/CatIsApple/GoRedis/internal/replica"
	"github.com/CatIsApple/GoRedis/internal/resp"
	"github.com/CatIsApple/GoRedis/internal/store"
)

// ListenAndServe는 addr에 TCP 리스너를 열고 클라이언트 연결을 받는다.
func ListenAndServe(ctx context.Context, addr, aofPath string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return ServeWithAOF(ctx, listener, aofPath)
}

// ListenAndServeReplica는 클라이언트 읽기용 포트를 열고 지정된 primary를 따라간다.
func ListenAndServeReplica(ctx context.Context, addr, primary string) error {
	if _, _, err := net.SplitHostPort(primary); err != nil {
		return err
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return serve(ctx, listener, "", primary)
}

// Serve는 주어진 리스너를 사용해 메모리 전용 서버를 실행한다.
// 리스너의 소유권은 서버로 넘어가며 종료 시 열린 연결과 함께 닫힌다.
func Serve(ctx context.Context, listener net.Listener) error {
	return ServeWithAOF(ctx, listener, "")
}

// ServeWithAOF는 복구를 마친 뒤 요청을 받는다. 빈 경로면 메모리에서만 실행한다.
func ServeWithAOF(ctx context.Context, listener net.Listener, path string) error {
	return serve(ctx, listener, path, "")
}

func serve(ctx context.Context, listener net.Listener, path, primary string) (err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var executor *engine.Engine
	var wg sync.WaitGroup
	if primary != "" {
		executor = engine.NewFollower(ctx)
		wg.Go(func() { replica.Follow(ctx, primary, executor) })
	} else {
		db := store.New()
		var journal engine.Journal
		if path != "" {
			// 같은 db에 AOF를 처음부터 재생한다. 복구가 끝나야 아래 Accept 루프에
			// 진입하므로 클라이언트가 복구 중인 키 공간을 읽을 수 없다.
			aof, err := persist.Open(path, db)
			if err != nil {
				listener.Close()
				return err
			}
			journal = aof
			defer func() {
				if closeErr := aof.Close(); closeErr != nil {
					err = errors.Join(err, closeErr)
				}
			}()
		}
		executor = engine.NewWithStore(ctx, db, journal)
	}
	wg.Go(func() {
		<-ctx.Done()
		listener.Close() // Accept의 대기를 해제한다.
	})
	defer func() {
		// 먼저 연결을 깨우고, 연결 고루틴과 실행기가 끝나기를 기다린다.
		// 앞에서 등록한 aof.Close는 defer의 역순 실행에 따라 그 뒤에 호출된다.
		cancel()
		wg.Wait()
		executor.Wait()
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
		// 각 연결 고루틴은 같은 executor를 사용하므로 모든 연결이 키 공간을 공유한다.
		wg.Go(func() { handleConn(ctx, conn, executor, primary == "") })
	}
}

func handleConn(ctx context.Context, conn net.Conn, executor *engine.Engine, isPrimary bool) {
	defer conn.Close()
	// 서버 종료 때 소켓을 닫으면 ReadValue나 Flush에서 대기 중인 연결도 깨어난다.
	// 연결이 먼저 끝나면 stop으로 종료 콜백을 해제한다.
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()

	// Reader가 다음 요청의 바이트까지 미리 읽을 수 있어 연결 내에서 계속 재사용한다.
	reader := resp.NewReader(conn)
	writer := resp.NewWriter(conn)
	for {
		command, err := reader.ReadValue()
		if err != nil {
			// 메시지 사이의 EOF는 클라이언트가 정상적으로 끊은 경우다.
			// 프레임이 잘렸거나 형식이 깨졌다면 오류 응답을 시도한 뒤 이 연결을 끝낸다.
			if errors.Is(err, resp.ErrProtocol) || errors.Is(err, io.ErrUnexpectedEOF) {
				writeReply(writer, resp.Value{Kind: resp.SimpleError, Bytes: []byte("ERR Protocol error")})
			}
			return
		}
		// 복제 요청을 받으면 이 소켓은 스냅샷과 변경분을 보내는 전용 연결이 된다.
		// 일반 클라이언트는 이 경로를 통해 replica의 저장소를 변경할 수 없다.
		if command.Kind == resp.Array && len(command.Elements) == 1 {
			name := command.Elements[0]
			if name.Kind == resp.BulkString && bytes.EqualFold(name.Bytes, []byte(replica.SyncCommand)) {
				if isPrimary {
					replica.Stream(ctx, conn, executor)
				} else {
					writeReply(writer, resp.Value{Kind: resp.SimpleError, Bytes: []byte("ERR replication source must be a primary")})
				}
				return
			}
		}
		// 여기서 완성된 명령 하나를 넘기고 그 결과를 기다린다. 현재 응답을
		// 쓴 뒤 다음 요청으로 넘어가므로 같은 연결의 파이프라인 순서가 보존된다.
		reply, ok := executor.Execute(command)
		if !ok || writeReply(writer, reply) != nil {
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
