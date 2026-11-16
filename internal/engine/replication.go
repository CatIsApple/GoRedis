package engine

import "github.com/CatIsApple/GoRedis/internal/store"

// Subscribe는 같은 실행 차례에 스냅샷을 만들고 구독을 등록한다. 스냅샷을
// 네트워크로 전송하는 사이 적용된 쓰기는 이 채널에 실행 순서대로 쌓인다.
func (e *Engine) Subscribe() (snapshot []store.Entry, changes <-chan store.Entry, unsubscribe func(), ok bool) {
	var ch chan store.Entry
	e.call(func(s *state) {
		if s.follower {
			return
		}
		snapshot = s.db.Entries()
		ch = make(chan store.Entry, 256)
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
	entry := s.db.Entry(key)
	for ch := range s.subscribers {
		select {
		case ch <- entry:
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

// ApplyReplica는 외부 클라이언트의 쓰기 명령과 구분해 복제 스트림의 변경을 적용한다.
func (e *Engine) ApplyReplica(entry store.Entry) (applied bool) {
	e.call(func(s *state) {
		if s.follower && s.db != nil {
			s.db.ApplyEntry(entry)
			applied = true
		}
	})
	return applied
}
