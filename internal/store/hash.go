package store

import (
	"errors"
	"time"
)

var ErrWrongType = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")

// HGet은 없는 키와 없는 필드에 같은 결과를 반환한다. 문자열 키에 조회하면 타입 오류다.
func (s *Store) HGet(key, field string) (string, bool, error) {
	return s.HGetAt(key, field, time.Now().UnixMilli())
}

func (s *Store) HGetAt(key, field string, now int64) (string, bool, error) {
	v, found := s.lookup(key, now)
	if !found {
		return "", false, nil
	}
	if v.fields == nil {
		return "", false, ErrWrongType
	}
	value, found := v.fields[field]
	return value, found, nil
}

// HSet은 호출자가 키의 타입과 만료 여부를 확인한 뒤 호출한다. 여기서 현재
// 시각을 다시 확인하면 AOF를 재생할 때 저장 당시 살아 있던 필드를 잃을 수 있다.
func (s *Store) HSet(key string, fields []Pair) int64 {
	if len(fields) == 0 {
		return 0
	}
	hash := s.values[key].fields
	if hash == nil {
		hash = make(map[string]string)
		s.values[key] = storedValue{fields: hash}
	}
	var added int64
	for _, field := range fields {
		if _, exists := hash[field.Key]; !exists {
			added++
		}
		hash[field.Key] = field.Value
	}
	return added
}
