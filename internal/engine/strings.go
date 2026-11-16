package engine

import (
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
