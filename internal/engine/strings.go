package engine

import (
	"math"
	"strconv"

	"github.com/CatIsApple/GoRedis/internal/resp"
)

// GET과 MGET 모두 없는 키는 null, 저장된 빈 문자열은 길이 0인 벌크 문자열로 응답한다.
func bulkReply(value string, found bool) resp.Value {
	if !found {
		return resp.Value{Kind: resp.BulkString}
	}
	return resp.Value{Kind: resp.BulkString, Bytes: []byte(value)}
}

// GET과 HGET은 키의 타입이 맞지 않으면 WRONGTYPE을, 값이 없으면 null을 보낸다.
func checkedBulkReply(value string, found bool, err error) resp.Value {
	if err != nil {
		return commandError(err.Error())
	}
	return bulkReply(value, found)
}

func (s *state) incr(key string, now int64) resp.Value {
	// 만료된 키는 0에서 시작한다. 숫자를 해석하기 전에 기존 키의 타입을 검사한다.
	value, found, err := s.db.StringAt(key, now)
	if err != nil {
		return commandError(err.Error())
	}
	var n int64
	if found {
		var ok bool
		n, ok = parseInt(value)
		if !ok {
			return commandError(integerError)
		}
		if n == math.MaxInt64 {
			return commandError("ERR increment or decrement would overflow")
		}
	}
	n++
	if err := s.writeString(key, strconv.FormatInt(n, 10), s.db.ExpiresAt(key)); err != nil {
		return commandError(journalError)
	}
	return resp.Value{Kind: resp.Integer, Int: n}
}
