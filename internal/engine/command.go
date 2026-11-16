package engine

import (
	"bytes"
	"strconv"

	"github.com/CatIsApple/GoRedis/internal/resp"
)

// execute는 연결이 해석한 RESP 배열에서 첫 원소를 명령 이름으로, 나머지를 인자로 읽는다.
// 명령 형식과 인자 수를 확인한 뒤에만 키 공간이나 AOF를 변경한다.
func (s *state) execute(command resp.Value) resp.Value {
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
	if s.follower {
		switch {
		case commandIs(name, "SET"), commandIs(name, "EXPIRE"):
			return commandError("READONLY You can't write against a read only replica")
		case s.db == nil && (commandIs(name, "GET") || commandIs(name, "TTL")):
			return commandError("LOADING Replica is waiting for its initial snapshot")
		}
	}
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
		if len(args) != 3 {
			return wrongArity("set")
		}
		key, value := string(args[1].Bytes), string(args[2].Bytes)
		// 로그 기록이 실패하면 기존 값과 TTL을 보존하고 이 요청에는 오류를 돌려준다.
		if s.journal != nil && s.journal.AppendSet(key, value) != nil {
			return commandError("MISCONF failed to persist write to AOF")
		}
		s.db.Set(key, value)
		s.publish(key)
		return resp.Value{Kind: resp.SimpleString, Bytes: []byte("OK")}
	case commandIs(name, "GET"):
		if len(args) != 2 {
			return wrongArity("get")
		}
		value, found := s.db.Get(string(args[1].Bytes))
		// 없는 키는 Bytes가 nil인 bulk string($-1), 저장된 빈 문자열은 길이 0인 bulk string($0)이다.
		if !found {
			return resp.Value{Kind: resp.BulkString}
		}
		return resp.Value{Kind: resp.BulkString, Bytes: []byte(value)}
	case commandIs(name, "EXPIRE"):
		if len(args) != 3 {
			return wrongArity("expire")
		}
		raw := string(args[2].Bytes)
		seconds, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || strconv.FormatInt(seconds, 10) != raw {
			return commandError("ERR value is not an integer or out of range")
		}
		key := string(args[1].Bytes)
		// 10초 뒤 만료되는 키를 3초 후 복구하면 약 7초가 남아야 한다.
		// 여기서 한 번 계산한 절대 시각을 디스크와 메모리 양쪽에 똑같이 적용한다.
		deadline, found, err := s.db.PrepareExpiry(key, seconds)
		if err != nil {
			return commandError("ERR invalid expire time in 'expire' command")
		}
		if !found {
			return resp.Value{Kind: resp.Integer, Int: 0}
		}
		if s.journal != nil && s.journal.AppendExpireAt(key, deadline) != nil {
			return commandError("MISCONF failed to persist write to AOF")
		}
		s.db.SetExpiryAt(key, deadline)
		s.publish(key)
		return resp.Value{Kind: resp.Integer, Int: 1}
	case commandIs(name, "TTL"):
		if len(args) != 2 {
			return wrongArity("ttl")
		}
		return resp.Value{Kind: resp.Integer, Int: s.db.TTL(string(args[1].Bytes))}
	default:
		return commandError("ERR unknown command")
	}
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
