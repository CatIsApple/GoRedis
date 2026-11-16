package engine

import (
	"context"
	"log"
	"time"

	"github.com/CatIsApple/GoRedis/internal/persist"
	"github.com/CatIsApple/GoRedis/internal/resp"
	"github.com/CatIsApple/GoRedis/internal/store"
)

// Journal은 한 쓰기 명령으로 생긴 변경을 현재 증분 AOF에 기록한다.
// 기록에 성공하면 실행기가 메모리에 같은 변경을 적용하고 클라이언트에 응답한다.
type Journal interface {
	AppendSet(key, value string) error
	AppendSetExpireAt(key, value string, deadlineMS int64) error
	AppendMSet(pairs []store.Pair) error
	AppendHSet(key string, fields []store.Pair, create bool) error
	AppendDel(keys []string) error
	AppendExpireAt(key string, deadlineMS int64) error
}

// 스냅샷 경계를 명령 사이에 잡아야 같은 변경이 RDB와 증분 AOF에 중복되지 않는다.
type rewriter interface {
	ShouldRewrite() bool
	Rewrite(db *store.Store) error
}

var _ rewriter = (*persist.AOF)(nil)

// Engine은 클라이언트 명령과 복제 작업을 하나의 고루틴에서 차례로 실행한다.
type Engine struct {
	requests chan func(*state)
	done     chan struct{}
}

// state에 대한 읽기와 쓰기는 실행 고루틴 한 곳에서만 일어난다.
// 복제본에서 db가 nil이면 아직 첫 스냅샷을 다 받지 못한 상태다.
type state struct {
	db          *store.Store
	journal     Journal
	rewriter    rewriter
	follower    bool
	subscribers map[chan Change]struct{}
}

func New(ctx context.Context) *Engine {
	return NewWithStore(ctx, store.New(), nil)
}

// NewWithStore는 시작 시 준비한 키 공간의 소유권을 primary 실행 고루틴에 넘긴다.
func NewWithStore(ctx context.Context, db *store.Store, journal Journal) *Engine {
	r, _ := journal.(rewriter)
	return start(ctx, &state{db: db, journal: journal, rewriter: r, subscribers: make(map[chan Change]struct{})})
}

// NewFollower는 첫 전체 동기화를 기다리는 읽기 전용 실행기를 만든다.
func NewFollower(ctx context.Context) *Engine {
	return start(ctx, &state{follower: true})
}

func start(ctx context.Context, s *state) *Engine {
	e := &Engine{requests: make(chan func(*state)), done: make(chan struct{})}
	go e.run(ctx, s)
	return e
}

func (e *Engine) run(ctx context.Context, s *state) {
	defer close(e.done)
	sweep := time.NewTicker(100 * time.Millisecond)
	defer sweep.Stop()
	for {
		select {
		case <-ctx.Done():
			for ch := range s.subscribers {
				close(ch)
			}
			return
		case <-sweep.C:
			if s.db != nil {
				s.db.PurgeExpired(20)
			}
			if s.rewriter != nil && s.rewriter.ShouldRewrite() {
				if err := s.rewriter.Rewrite(s.db); err != nil {
					log.Printf("automatic AOF rewrite: %v", err)
				}
			}
		case work := <-e.requests:
			work(s)
		}
	}
}

// call은 한 작업이 결과를 기록할 때까지 기다린다. 각 호출이 자기 finished를
// 사용하므로 동시 요청의 응답이 섞이지 않는다. 닫기는 수신자를 기다리지 않는다.
func (e *Engine) call(work func(*state)) bool {
	finished := make(chan struct{})
	select {
	case e.requests <- func(s *state) {
		work(s)
		close(finished)
	}:
		<-finished
		return true
	case <-e.done:
		return false
	}
}

// Execute는 명령의 응답을 기다린다. 실행기가 종료됐으면 ok는 false다.
func (e *Engine) Execute(command resp.Value) (reply resp.Value, ok bool) {
	ok = e.call(func(s *state) { reply = s.execute(command, time.Now().UnixMilli()) })
	return reply, ok
}

// Wait는 실행기가 구독 채널까지 정리하고 종료될 때까지 기다린다.
func (e *Engine) Wait() {
	<-e.done
}
