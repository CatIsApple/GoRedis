package replica

import (
	"context"
	"errors"
	"log"
	"net"
	"time"

	"github.com/CatIsApple/GoRedis/internal/engine"
	"github.com/CatIsApple/GoRedis/internal/resp"
	"github.com/CatIsApple/GoRedis/internal/store"
)

// Follow는 전용 TCP 연결을 열고 끊기면 다시 구독한다. 재시도 중에도 기존 키 공간은
// 계속 읽을 수 있고, 새 스냅샷이 완성된 시점에만 교체된다.
func Follow(ctx context.Context, primary string, executor *engine.Engine) {
	const retryMin = 250 * time.Millisecond
	delay := retryMin
	var lastLog time.Time
	dialer := net.Dialer{Timeout: 5 * time.Second}
	for ctx.Err() == nil {
		conn, err := dialer.DialContext(ctx, "tcp", primary)
		if err == nil {
			// 취소 시 블로킹 중인 소켓 읽기도 즉시 해제한다.
			stop := context.AfterFunc(ctx, func() { conn.Close() })
			start := time.Now()
			err = receive(conn, executor)
			stop()
			conn.Close()
			if time.Since(start) > frameTimeout {
				delay = retryMin
			}
		}
		if ctx.Err() != nil {
			return
		}
		if time.Since(lastLog) >= 10*time.Second {
			log.Printf("replica: retrying primary %s: %v", primary, err)
			lastLog = time.Now()
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
			delay = min(2*delay, 3*time.Second)
		}
	}
}

func receive(conn net.Conn, executor *engine.Engine) error {
	req := resp.Value{Kind: resp.Array, Elements: []resp.Value{
		{Kind: resp.BulkString, Bytes: []byte(SyncCommand)},
	}}
	if err := writeFrame(conn, resp.NewWriter(conn), req); err != nil {
		return err
	}
	reader := resp.NewReader(conn)
	frame, err := readFrame(conn, reader)
	if err != nil {
		return err
	}
	if !isMarker(frame, fullSync) {
		return errors.New("replication stream: expected FULLSYNC")
	}

	// 아직 사용자에게 보이지 않는 별도 저장소다. READY 전에 EOF가 오면 함수가
	// 반환되며 미완성 스냅샷도 함께 버려진다.
	staging := store.New()
	for {
		frame, err = readFrame(conn, reader)
		if err != nil {
			return err
		}
		if staging != nil && isMarker(frame, ready) {
			if !executor.ReplaceReplica(staging) {
				return errors.New("replica executor unavailable")
			}
			staging = nil // 이후 변경은 실행기를 거쳐 공개된 키 공간에 하나씩 적용한다.
			continue
		}
		if staging == nil && isMarker(frame, ping) {
			continue
		}
		entry, err := parseEntry(frame)
		if err != nil {
			return err
		}
		if staging != nil {
			if entry.Deleted {
				return errors.New("replication stream: deletion inside snapshot")
			}
			staging.ApplyEntry(entry)
		} else if !executor.ApplyReplica(entry) {
			return errors.New("replica executor unavailable")
		}
	}
}
