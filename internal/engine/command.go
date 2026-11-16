package engine

import (
	"bytes"
	"strconv"

	"github.com/CatIsApple/GoRedis/internal/resp"
)

// execute는 연결이 해석한 RESP 배열에서 첫 원소를 명령 이름으로, 나머지를 인자로 읽는다.
// 명령 형식과 인자 수를 확인한 뒤에만 키 공간이나 AOF를 변경한다.
func (s *state) execute(command resp.Value, now int64) resp.Value {
	args := command.Elements
	if command.Kind != resp.Array || len(args) == 0 {
		return commandError("ERR expected a non-empty command array")
	}
	for _, arg := range args {
		if arg.Kind != resp.BulkString || arg.Bytes == nil {
			return commandError("ERR expected a non-null bulk string")
		}
	}

	name := args[0].Bytes
	switch {
	case commandIs(name, "PING"):
		if len(args) == 2 {
			return args[1]
		}
		if len(args) != 1 {
			return wrongArity("ping")
		}
		return resp.Value{Kind: resp.SimpleString, Bytes: []byte("PONG")}
	case commandIs(name, "SET"):
		if len(args) < 3 {
			return wrongArity("set")
		}
		if s.follower {
			return commandError(readOnlyError)
		}
		return s.set(args, now)
	case commandIs(name, "GET"):
		if len(args) != 2 {
			return wrongArity("get")
		}
		if s.db == nil {
			return commandError(loadingError)
		}
		return checkedBulkReply(s.db.StringAt(string(args[1].Bytes), now))
	case commandIs(name, "TYPE"):
		if len(args) != 2 {
			return wrongArity("type")
		}
		if s.db == nil {
			return commandError(loadingError)
		}
		kind := s.db.TypeAt(string(args[1].Bytes), now)
		return resp.Value{Kind: resp.SimpleString, Bytes: []byte(kind)}
	case commandIs(name, "EXPIRE"):
		if len(args) != 3 {
			return wrongArity("expire")
		}
		if s.follower {
			return commandError(readOnlyError)
		}
		seconds, ok := parseInt(string(args[2].Bytes))
		if !ok {
			return commandError(integerError)
		}
		key := string(args[1].Bytes)
		// 10초 뒤 만료되는 키를 3초 후 복구하면 약 7초가 남아야 한다.
		// 여기서 한 번 계산한 절대 시각을 디스크와 메모리 양쪽에 똑같이 적용한다.
		deadline, found, err := s.db.PrepareExpiryAt(key, seconds, now)
		if err != nil {
			return commandError("ERR invalid expire time in 'expire' command")
		}
		if !found {
			return resp.Value{Kind: resp.Integer, Int: 0}
		}
		if s.journal != nil && s.journal.AppendExpireAt(key, deadline) != nil {
			return commandError(journalError)
		}
		s.db.SetExpiryAt(key, deadline)
		s.publish(key)
		return resp.Value{Kind: resp.Integer, Int: 1}
	case commandIs(name, "TTL"):
		if len(args) != 2 {
			return wrongArity("ttl")
		}
		if s.db == nil {
			return commandError(loadingError)
		}
		return resp.Value{Kind: resp.Integer, Int: s.db.TTLAt(string(args[1].Bytes), now)}
	default:
		return commandError("ERR unknown command")
	}
}

const (
	readOnlyError = "READONLY You can't write against a read only replica."
	loadingError  = "LOADING Replica is waiting for its initial snapshot"
	integerError  = "ERR value is not an integer or out of range"
	journalError  = "MISCONF failed to persist write to AOF"
)

// Redis 정수 인자는 부호 +, 선행 0, -0을 허용하지 않는다.
func parseInt(text string) (int64, bool) {
	// 가장 긴 int64 표기는 -9223372036854775808이다. 긴 문자열 값에 INCR을
	// 보내도 문자열 전체를 숫자로 변환하려고 하지 않는다.
	if len(text) > len("-9223372036854775808") {
		return 0, false
	}
	n, err := strconv.ParseInt(text, 10, 64)
	return n, err == nil && strconv.FormatInt(n, 10) == text
}

// 명령 이름은 ASCII에서만 대소문자를 구분하지 않는다.
func commandIs(name []byte, ascii string) bool {
	return len(name) == len(ascii) && bytes.EqualFold(name, []byte(ascii))
}

func wrongArity(name string) resp.Value {
	return commandError("ERR wrong number of arguments for '" + name + "' command")
}

func commandError(message string) resp.Value {
	return resp.Value{Kind: resp.SimpleError, Bytes: []byte(message)}
}
