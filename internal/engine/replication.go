package engine

import (
	"time"

	"github.com/CatIsApple/GoRedis/internal/store"
)

// Change는 primary에서 실행을 마친 명령 한 건이다. 단일 키는 Entry로,
// 다중 DEL과 MSET은 각각 삭제 목록과 키·값 쌍으로 한 번에 전달한다.
// HashUpdate이면 Entry.Hash에는 이번 HSET이 쓴 필드만 들어 있다.
type Change struct {
	Entry       store.Entry
	HashUpdate  bool
	DeletedKeys []string
	SetPairs    []store.Pair
}

// Subscribe는 같은 실행 차례에 스냅샷을 만들고 구독을 등록한다. 스냅샷을
// 네트워크로 전송하는 사이 적용된 쓰기는 이 채널에 실행 순서대로 쌓인다.
func (e *Engine) Subscribe() (snapshot []store.Entry, changes <-chan Change, unsubscribe func(), ok bool) {
	var ch chan Change
	e.call(func(s *state) {
		if s.follower {
			return
		}
		snapshot = s.db.Entries()
		ch = make(chan Change, 256)
		s.subscribers[ch] = struct{}{}
	})
	if ch == nil {
		return nil, nil, nil, false
	}
	return snapshot, ch, func() {
		e.call(func(s *state) {
			if _, exists := s.subscribers[ch]; exists {
				delete(s.subscribers, ch)
				close(ch)
			}
		})
	}, true
}

func (s *state) publish(key string) {
	if len(s.subscribers) == 0 {
		return
	}
	// EXPIRE도 값과 기한을 함께 보낸다. 복제본에서 이전 기한이 먼저 지났어도
	// 뒤늦게 도착한 연장 통지로 primary의 최신 값을 복원할 수 있다.
	s.broadcast(Change{Entry: s.db.Entry(key)})
}

// 처음 생성한 해시는 이전 키를 통째로 교체한다. 이미 존재하던 해시의 HSET은
// 요청에 들어온 필드만 보내므로 필드 하나를 바꿀 때 전체 맵을 복사하지 않는다.
func (s *state) publishHash(key string, fields []store.Pair, create bool, deadline int64) {
	s.broadcast(Change{
		Entry:      store.Entry{Key: key, Hash: fields, ExpiresAt: deadline},
		HashUpdate: !create,
	})
}

func (s *state) publishDeleted(keys []string) {
	switch len(keys) {
	case 0:
		return
	case 1:
		s.broadcast(Change{Entry: store.Entry{Key: keys[0], Deleted: true}})
	default:
		// 명령이 만든 삭제 목록은 이후 수정하지 않으므로 모든 구독자가 함께 읽을 수 있다.
		s.broadcast(Change{DeletedKeys: keys})
	}
}

func (s *state) publishMSet(pairs []store.Pair) {
	if len(pairs) == 1 {
		s.publish(pairs[0].Key)
		return
	}
	// 실행이 끝난 인자 목록은 이후 바뀌지 않는다. 모든 구독자가 MSET 한 건을
	// 같은 순서로 받으므로 중복 키의 최종 값도 primary와 일치한다.
	s.broadcast(Change{SetPairs: pairs})
}

func (s *state) broadcast(change Change) {
	for ch := range s.subscribers {
		select {
		case ch <- change:
		default:
			// 느린 연결 때문에 명령 실행이 막히면 모든 클라이언트가 영향을 받는다.
			// 한 건이라도 빠지면 이 스트림을 닫아 복제본이 전체 동기화부터 다시 시작하게 한다.
			delete(s.subscribers, ch)
			close(ch)
		}
	}
}

// ReplaceReplica는 완성된 스냅샷의 소유권을 실행기에 넘긴다. 교체 전까지는
// 클라이언트가 기존 스냅샷을 읽을 수 있고, 일부만 수신된 키 공간은 노출되지 않는다.
func (e *Engine) ReplaceReplica(db *store.Store) (applied bool) {
	e.call(func(s *state) {
		if s.follower && db != nil {
			s.db = db
			applied = true
		}
	})
	return applied
}

// ApplyReplica는 변경 한 건을 실행기에 맡긴다. MSET이나 DEL의 모든 키를
// 반영한 뒤에야 다른 클라이언트의 MGET 같은 조회 명령을 실행한다.
func (e *Engine) ApplyReplica(change Change) (applied bool) {
	e.call(func(s *state) {
		if !s.follower || s.db == nil {
			return
		}
		switch {
		case change.HashUpdate:
			entry := change.Entry
			now := time.Now().UnixMilli()
			kind := s.db.TypeAt(entry.Key, now)
			if kind == "none" && entry.ExpiresAt != 0 && entry.ExpiresAt <= now {
				// 복제본에서도 기한이 지난 키의 필드는 다시 만들지 않는다.
				break
			}
			// HSET은 기존 해시와 그 만료 시각을 보존한다. 바탕이 되는 상태가
			// 다르면 일부 필드만으로 복구할 수 없어 재동기화한다.
			if kind != "hash" || s.db.ExpiresAt(entry.Key) != entry.ExpiresAt {
				return
			}
			s.db.HSet(entry.Key, entry.Hash)
		case len(change.SetPairs) != 0:
			s.db.SetMany(change.SetPairs)
		case len(change.DeletedKeys) != 0:
			for _, key := range change.DeletedKeys {
				s.db.Delete(key)
			}
		default:
			s.db.ApplyEntry(change.Entry)
		}
		applied = true
	})
	return applied
}
