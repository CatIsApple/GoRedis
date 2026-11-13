package replica

import (
	"errors"
	"net"
	"time"

	"github.com/CatIsApple/GoRedis/internal/resp"
	"github.com/CatIsApple/GoRedis/internal/store"
)

// SyncCommand는 일반 요청 연결을 노드 간 복제 스트림으로 전환하는 명령이다.
const SyncCommand = "GOREDIS.SYNC"

const (
	fullSync = "FULLSYNC"
	ready    = "READY"
	ping     = "PING"
	// 변경이 없을 때에도 보내는 PING 간격보다 길게 잡는다.
	frameTimeout = 30 * time.Second
)

func marker(name string) resp.Value {
	return resp.Value{Kind: resp.SimpleString, Bytes: []byte(name)}
}

func isMarker(frame resp.Value, name string) bool {
	return frame.Kind == resp.SimpleString && string(frame.Bytes) == name
}

// 각 변경은 해당 키의 최종 상태를 담는다. 세 번째 원소는 절대 만료 시각(ms)이며,
// 0은 영구 키다. 삭제는 null 값으로 인코딩해 길이 0인 실제 값과 구별한다.
func entryFrame(entry store.Entry) resp.Value {
	value := resp.Value{Kind: resp.BulkString}
	if !entry.Deleted {
		value.Bytes = []byte(entry.Value)
	}
	return resp.Value{Kind: resp.Array, Elements: []resp.Value{
		{Kind: resp.BulkString, Bytes: []byte(entry.Key)},
		value,
		{Kind: resp.Integer, Int: entry.ExpiresAt},
	}}
}

func parseEntry(frame resp.Value) (store.Entry, error) {
	if frame.Kind != resp.Array || len(frame.Elements) != 3 {
		return store.Entry{}, errors.New("replication stream: expected a three-element entry")
	}
	key, value, expiry := frame.Elements[0], frame.Elements[1], frame.Elements[2]
	if key.Kind != resp.BulkString || key.Bytes == nil || value.Kind != resp.BulkString ||
		expiry.Kind != resp.Integer || expiry.Int < 0 || (value.Bytes == nil && expiry.Int != 0) {
		return store.Entry{}, errors.New("replication stream: invalid key, value or expiry")
	}
	return store.Entry{Key: string(key.Bytes), Value: string(value.Bytes), ExpiresAt: expiry.Int, Deleted: value.Bytes == nil}, nil
}

func writeFrame(conn net.Conn, w *resp.Writer, frame resp.Value) error {
	if err := conn.SetWriteDeadline(time.Now().Add(frameTimeout)); err != nil {
		return err
	}
	if err := w.WriteValue(frame); err != nil {
		return err
	}
	return w.Flush()
}

func readFrame(conn net.Conn, r *resp.Reader) (resp.Value, error) {
	if err := conn.SetReadDeadline(time.Now().Add(frameTimeout)); err != nil {
		return resp.Value{}, err
	}
	return r.ReadValue()
}
