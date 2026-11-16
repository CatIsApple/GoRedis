package store

import "time"

// Entry는 한 키의 값과 만료 시각을 함께 담는다. ExpiresAt은 Unix 밀리초이며
// 0이면 만료 설정이 없다. Deleted는 빈 문자열을 저장한 경우와 삭제를 구분한다.
type Entry struct {
	Key       string
	Value     string
	ExpiresAt int64
	Deleted   bool
}

// Entry는 쓰기가 끝난 뒤 복제본에 전달할 키의 현재 상태를 돌려준다.
func (s *Store) Entry(key string) Entry {
	return s.entryAt(key, time.Now().UnixMilli())
}

func (s *Store) entryAt(key string, now int64) Entry {
	value, found := s.lookup(key, now)
	if !found {
		return Entry{Key: key, Deleted: true}
	}
	return Entry{Key: key, Value: value, ExpiresAt: s.deadlines[key]}
}

// Entries는 한 시점에 살아 있는 키만 복사한다. 결과의 문자열은 불변이라
// 다른 고루틴이 스냅샷을 전송하는 동안에도 실행기는 자기 맵을 갱신할 수 있다.
func (s *Store) Entries() []Entry {
	now := time.Now().UnixMilli()
	entries := make([]Entry, 0, len(s.values))
	for key := range s.values {
		entry := s.entryAt(key, now)
		if !entry.Deleted {
			entries = append(entries, entry)
		}
	}
	return entries
}

// ApplyEntry는 primary에서 결정한 절대 만료 시각을 그대로 사용한다.
// 이미 지나간 시각이라면 네트워크에서 보낸 시간만큼 키가 더 살아나는 일을 막는다.
func (s *Store) ApplyEntry(entry Entry) {
	if entry.Deleted || (entry.ExpiresAt != 0 && entry.ExpiresAt <= time.Now().UnixMilli()) {
		s.remove(entry.Key)
		return
	}
	s.Set(entry.Key, entry.Value)
	if entry.ExpiresAt != 0 {
		s.deadlines[entry.Key] = entry.ExpiresAt
	}
}
