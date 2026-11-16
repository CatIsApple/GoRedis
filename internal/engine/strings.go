package engine

import (
	"math"
	"strconv"

	"github.com/CatIsApple/GoRedis/internal/resp"
	"github.com/CatIsApple/GoRedis/internal/store"
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

func (s *state) mget(keys []resp.Value, now int64) resp.Value {
	// 한 명령 안에서 같은 키가 반복되거나 도중에 만료 경계를 지나도 같은 시점으로 조회한다.
	values := make([]resp.Value, len(keys))
	for i, key := range keys {
		// MGET은 해시 키의 자리만 null로 채우고 나머지 키를 계속 조회한다.
		values[i] = bulkReply(s.db.GetAt(string(key.Bytes), now))
	}
	return resp.Value{Kind: resp.Array, Elements: values}
}

// MSET의 키·값과 HSET의 필드·값 모두 요청에 나온 순서대로 적용한다.
func parsePairs(args []resp.Value) []store.Pair {
	pairs := make([]store.Pair, len(args)/2)
	for i := range pairs {
		pairs[i] = store.Pair{Key: string(args[2*i].Bytes), Value: string(args[2*i+1].Bytes)}
	}
	return pairs
}

func (s *state) mset(args []resp.Value) resp.Value {
	pairs := parsePairs(args)
	// 한 레코드에 모든 쌍을 담아야 중단된 쓰기를 복구할 때 일부 키만 남지 않는다.
	if s.journal != nil && s.journal.AppendMSet(pairs) != nil {
		return commandError(journalError)
	}
	s.db.SetMany(pairs)
	s.publishMSet(pairs)
	return resp.Value{Kind: resp.SimpleString, Bytes: []byte("OK")}
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
