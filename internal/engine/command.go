package engine

import (
	"bytes"
	"strconv"

	"github.com/CatIsApple/GoRedis/internal/resp"
	"github.com/CatIsApple/GoRedis/internal/store"
)

// execute는 연결이 해석한 RESP 배열에서 첫 원소를 명령 이름으로, 나머지를 인자로 읽는다.
func execute(db *store.Store, command resp.Value) resp.Value {
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
		if len(args) != 3 {
			return wrongArity("set")
		}
		key, value := string(args[1].Bytes), string(args[2].Bytes)
		db.Set(key, value)
		return resp.Value{Kind: resp.SimpleString, Bytes: []byte("OK")}
	case commandIs(name, "GET"):
		if len(args) != 2 {
			return wrongArity("get")
		}
		value, found := db.Get(string(args[1].Bytes))
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
		applied, err := db.Expire(key, seconds)
		if err != nil {
			return commandError("ERR invalid expire time in 'expire' command")
		}
		if applied {
			return resp.Value{Kind: resp.Integer, Int: 1}
		}
		return resp.Value{Kind: resp.Integer, Int: 0}
	case commandIs(name, "TTL"):
		if len(args) != 2 {
			return wrongArity("ttl")
		}
		return resp.Value{Kind: resp.Integer, Int: db.TTL(string(args[1].Bytes))}
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
