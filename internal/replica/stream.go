package replica

import (
	"context"
	"net"
	"time"

	"github.com/CatIsApple/GoRedis/internal/engine"
	"github.com/CatIsApple/GoRedis/internal/resp"
)

// Stream은 primary의 연결 고루틴에서 실행된다. 스냅샷 전송 중의 변경도 구독
// 채널에 순서대로 쌓이므로 스냅샷의 마지막 키와 첫 실시간 변경 사이에 빈틈이 없다.
func Stream(ctx context.Context, conn net.Conn, executor *engine.Engine) {
	snapshot, changes, unsubscribe, ok := executor.Subscribe()
	if !ok {
		return
	}
	defer unsubscribe()

	w := resp.NewWriter(conn)
	if err := writeFrame(conn, w, marker(fullSync)); err != nil {
		return
	}
	for _, entry := range snapshot {
		// 스냅샷을 전부 받아야 조회에 쓰이므로 작은 키마다 Flush하지 않고
		// RESP 버퍼에 모아 보낸다. 마지막 READY가 남은 바이트까지 내보낸다.
		if err := conn.SetWriteDeadline(time.Now().Add(frameTimeout)); err != nil {
			return
		}
		if err := w.WriteValue(entryFrame(entry)); err != nil {
			return
		}
	}
	if err := writeFrame(conn, w, marker(ready)); err != nil {
		return
	}

	// 유휴 연결에서도 닫힌 소켓을 발견하고, replica 쪽은 PING 수신으로
	// 정상적인 무변경 상태와 끊긴 연결을 구별한다.
	heartbeat := time.NewTicker(2 * time.Second)
	defer heartbeat.Stop()
	for {
		var frame resp.Value
		select {
		case <-ctx.Done():
			return
		case <-heartbeat.C:
			frame = marker(ping)
		case entry, open := <-changes:
			if !open {
				// 구독 큐가 가득 차면 실행기는 이 구독을 종료한다. 연결을 닫으면
				// replica가 새 스냅샷을 요청해 빠진 변경까지 복구한다.
				return
			}
			frame = entryFrame(entry)
		}
		if err := writeFrame(conn, w, frame); err != nil {
			return
		}
	}
}
