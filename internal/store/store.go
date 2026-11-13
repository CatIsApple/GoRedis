package store

import (
	"errors"
	"math"
	"time"
)

// Store는 단일 실행기가 소유하는 인메모리 키 공간이다.
// 키와 값은 임의의 바이트를 보존하는 Go 문자열로 저장한다.
// deadlines에는 기한이 있는 키만 넣으므로 만료 정리는 이 맵만 순회한다.
type Store struct {
	values    map[string]string
	deadlines map[string]int64 // 만료가 설정된 키의 Unix 밀리초
}

func New() *Store {
	return &Store{values: make(map[string]string), deadlines: make(map[string]int64)}
}

func (s *Store) Set(key, value string) {
	s.values[key] = value
	// 덮어쓴 값이 예전 값의 만료 시각에 함께 사라지지 않도록 기한도 지운다.
	delete(s.deadlines, key)
}

func (s *Store) Get(key string) (string, bool) {
	return s.lookup(key, time.Now().UnixMilli())
}

// Expire는 키의 만료 시간을 갱신한다. 0초 이하이면 키를 바로 삭제한다.
func (s *Store) Expire(key string, seconds int64) (bool, error) {
	now := time.Now().UnixMilli()
	if seconds < math.MinInt64/1000 || seconds > math.MaxInt64/1000 || (seconds > 0 && now > math.MaxInt64-seconds*1000) {
		return false, errors.New("expiration time out of range")
	}
	_, found := s.lookup(key, now)
	if !found {
		return false, nil
	}
	if seconds <= 0 {
		s.remove(key)
	} else {
		s.deadlines[key] = now + seconds*1000
	}
	return true, nil
}

// TTL은 남은 초를 반올림해 반환한다. 키가 없으면 -2, 만료 설정이 없으면 -1이다.
func (s *Store) TTL(key string) int64 {
	now := time.Now().UnixMilli()
	_, found := s.lookup(key, now)
	if !found {
		return -2
	}
	deadline, expires := s.deadlines[key]
	if !expires {
		return -1
	}
	remaining := deadline - now
	// 500ms가 남으면 1, 499ms가 남으면 0이다. TTL이 0이어도 실제 기한 전까지 GET은 값을 돌려준다.
	// 나머지에만 500을 더하면 아주 먼 기한도 int64 범위에서 반올림할 수 있다.
	return remaining/1000 + (remaining%1000+500)/1000
}

// PurgeExpired는 만료 시간이 있는 키를 최대 limit개 검사해 지난 키를 회수한다.
func (s *Store) PurgeExpired(limit int) {
	now := time.Now().UnixMilli()
	for key, deadline := range s.deadlines {
		if limit <= 0 {
			break
		}
		if deadline <= now {
			s.remove(key)
		}
		limit--
	}
}

func (s *Store) lookup(key string, now int64) (string, bool) {
	// 주기적인 정리가 아직 이 키에 도달하지 않았어도, 지금 만료됐다면 이 조회에서 바로 제거한다.
	if deadline, expires := s.deadlines[key]; expires && deadline <= now {
		s.remove(key)
		return "", false
	}
	value, found := s.values[key]
	return value, found
}

func (s *Store) remove(key string) {
	delete(s.values, key)
	delete(s.deadlines, key)
}
