package engine

import (
	"github.com/CatIsApple/GoRedis/internal/resp"
	"github.com/CatIsApple/GoRedis/internal/store"
)

// ClusterMachine은 한 샤드의 커밋된 로그를 순서대로 적용한다. Raft의 FSM
// 고루틴이 Apply와 Snapshot 호출을 직렬화하므로 별도의 명령 큐가 필요 없다.
type ClusterMachine struct {
	state  state
	lastMS int64
}

func NewClusterMachine() *ClusterMachine {
	return &ClusterMachine{state: state{db: store.New()}}
}

// Apply는 primary가 로그에 담은 시각을 사용한다. 늦게 따라잡는 replica도
// EXPIRE, SET NX, INCR 등에서 원래 명령 시점에 살아 있던 키를 동일하게 본다.
func (m *ClusterMachine) Apply(args []string, atMS int64) resp.Value {
	// 동시에 도착한 요청의 커밋 순서나 새 primary의 시계 때문에 시간이 뒤로
	// 갈 수 있다. 한 번 만료된 키가 다시 보이지 않도록 마지막 시각을 보존한다.
	m.lastMS = max(m.lastMS, atMS)
	values := make([]resp.Value, len(args))
	for i, arg := range args {
		values[i] = resp.Value{Kind: resp.BulkString, Bytes: []byte(arg)}
	}
	return m.state.execute(resp.Value{Kind: resp.Array, Elements: values}, m.lastMS)
}

// Snapshot은 마지막 로그와 같은 시점의 값과 절대 만료 시각을 복사한다.
// 반환 후 파일을 쓰는 동안 FSM은 다음 명령을 계속 처리할 수 있다.
func (m *ClusterMachine) Snapshot() ([]store.Entry, int64) {
	return m.state.db.EntriesAt(m.lastMS), m.lastMS
}

// Restore는 같은 스냅샷에 기록한 키 공간과 마지막 명령 시각을 함께 설치한다.
func (m *ClusterMachine) Restore(db *store.Store, lastMS int64) {
	m.state.db = db
	m.lastMS = lastMS
}
