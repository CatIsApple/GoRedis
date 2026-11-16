package replica

import (
	"errors"
	"net"
	"time"

	"github.com/CatIsApple/GoRedis/internal/engine"
	"github.com/CatIsApple/GoRedis/internal/resp"
	"github.com/CatIsApple/GoRedis/internal/store"
)

// SyncCommand는 일반 요청 연결을 노드 간 복제 스트림으로 전환하는 명령이다.
const SyncCommand = "GOREDIS.SYNC"

const (
	fullSync = "FULLSYNC"
	ready    = "READY"
	ping     = "PING"
	del      = "DEL"
	mset     = "MSET"
	// 변경이 없을 때에도 보내는 PING 간격보다 길게 잡는다.
	frameTimeout = 30 * time.Second
)

func marker(name string) resp.Value {
	return resp.Value{Kind: resp.SimpleString, Bytes: []byte(name)}
}

func isMarker(frame resp.Value, name string) bool {
	return frame.Kind == resp.SimpleString && string(frame.Bytes) == name
}

// 문자열과 삭제는 기존 세 원소 프레임으로 보낸다. 세 번째 원소는 절대 만료 시각(ms)이며,
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

// 다중 명령은 [+DEL, $key, ...] 또는 [+MSET, $key, $value, ...]로 보낸다.
// 첫 원소의 RESP 자료형이 단일 키 프레임의 벌크 문자열과 구분된다.
func changeFrame(change engine.Change) resp.Value {
	var args []resp.Value
	switch {
	case len(change.SetPairs) != 0:
		args = make([]resp.Value, 1, 1+2*len(change.SetPairs))
		args[0] = marker(mset)
		for _, pair := range change.SetPairs {
			args = append(args, resp.Value{Kind: resp.BulkString, Bytes: []byte(pair.Key)},
				resp.Value{Kind: resp.BulkString, Bytes: []byte(pair.Value)})
		}
	case len(change.DeletedKeys) != 0:
		args = make([]resp.Value, 1, len(change.DeletedKeys)+1)
		args[0] = marker(del)
		for _, key := range change.DeletedKeys {
			args = append(args, resp.Value{Kind: resp.BulkString, Bytes: []byte(key)})
		}
	default:
		return entryFrame(change.Entry)
	}
	return resp.Value{Kind: resp.Array, Elements: args}
}

func parseChange(frame resp.Value) (engine.Change, error) {
	args := frame.Elements
	if frame.Kind != resp.Array || len(args) == 0 || args[0].Kind != resp.SimpleString {
		entry, err := parseEntry(frame)
		return engine.Change{Entry: entry}, err
	}
	for _, arg := range args[1:] {
		if arg.Kind != resp.BulkString || arg.Bytes == nil {
			return engine.Change{}, errors.New("replication stream: expected a non-null bulk string")
		}
	}
	switch string(args[0].Bytes) {
	case mset:
		if len(args) < 3 || len(args)%2 == 0 {
			return engine.Change{}, errors.New("replication stream: MSET requires key-value pairs")
		}
		pairs := make([]store.Pair, 0, len(args)/2)
		for i := 1; i < len(args); i += 2 {
			pairs = append(pairs, store.Pair{Key: string(args[i].Bytes), Value: string(args[i+1].Bytes)})
		}
		return engine.Change{SetPairs: pairs}, nil
	case del:
		if len(args) < 2 {
			return engine.Change{}, errors.New("replication stream: DEL requires a key")
		}
		keys := make([]string, 0, len(args)-1)
		for _, key := range args[1:] {
			keys = append(keys, string(key.Bytes))
		}
		return engine.Change{DeletedKeys: keys}, nil
	default:
		return engine.Change{}, errors.New("replication stream: unknown change")
	}
}

func bufferFrame(conn net.Conn, w *resp.Writer, frame resp.Value) error {
	if err := conn.SetWriteDeadline(time.Now().Add(frameTimeout)); err != nil {
		return err
	}
	return w.WriteValue(frame)
}

func writeFrame(conn net.Conn, w *resp.Writer, frame resp.Value) error {
	if err := bufferFrame(conn, w, frame); err != nil {
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
