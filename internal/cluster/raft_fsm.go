package cluster

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/CatIsApple/GoRedis/internal/engine"
	"github.com/CatIsApple/GoRedis/internal/persist"
	"github.com/CatIsApple/GoRedis/internal/store"
	"github.com/hashicorp/raft"
)

// 요청 시각을 로그 본문에 넣어 모든 replica가 EXPIRE와 SET EX를 같은 시각에
// 실행한다. 인자는 바이트 길이로 구분해 잘못된 UTF-8, NUL과 CRLF도 보존한다.
func encodeCommand(args []string, atMS int64) []byte {
	data := binary.BigEndian.AppendUint64(nil, uint64(atMS))
	for _, arg := range args {
		data = binary.AppendUvarint(data, uint64(len(arg)))
		data = append(data, arg...)
	}
	return data
}

func decodeCommand(data []byte) ([]string, int64, error) {
	if len(data) <= 8 {
		return nil, 0, io.ErrUnexpectedEOF
	}
	atMS := int64(binary.BigEndian.Uint64(data))
	data = data[8:]
	var args []string
	for len(data) > 0 {
		length, n := binary.Uvarint(data)
		if n <= 0 || length > uint64(len(data)-n) {
			return nil, 0, fmt.Errorf("invalid raft argument length")
		}
		data = data[n:]
		args = append(args, string(data[:length]))
		data = data[length:]
	}
	return args, atMS, nil
}

type replicatedFSM struct{ machine *engine.ClusterMachine }

func (f *replicatedFSM) Apply(entry *raft.Log) interface{} {
	args, atMS, err := decodeCommand(entry.Data)
	if err != nil {
		// 이 경로에는 이 프로그램이 기록한 과반수 커밋 로그만 들어온다.
		// 손상된 항목을 건너뛰면 이 replica가 서로 다른 키 공간으로 승격될 수 있다.
		panic(fmt.Sprintf("invalid committed raft log %d: %v", entry.Index, err))
	}
	return f.machine.Apply(args, atMS)
}

func (f *replicatedFSM) Snapshot() (raft.FSMSnapshot, error) {
	entries, clock := f.machine.Snapshot()
	return &shardSnapshot{entries: entries, clock: clock}, nil
}

func (f *replicatedFSM) Restore(r io.ReadCloser) error {
	defer r.Close()
	var clock int64
	if err := binary.Read(r, binary.BigEndian, &clock); err != nil {
		return err
	}
	db, err := persist.ReadSnapshot(r)
	if err != nil {
		return err
	}
	f.machine.Restore(db, clock)
	return nil
}

type shardSnapshot struct {
	entries []store.Entry
	clock   int64
}

func (s *shardSnapshot) Persist(sink raft.SnapshotSink) error {
	// 키와 함께 마지막 논리 시각을 저장해야 재생 도중 만료 판단이 역행하지 않는다.
	if err := binary.Write(sink, binary.BigEndian, s.clock); err != nil {
		sink.Cancel()
		return err
	}
	if err := persist.WriteSnapshot(sink, s.entries); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *shardSnapshot) Release() { s.entries = nil }
