package engine

import (
	"bytes"

	"github.com/CatIsApple/GoRedis/internal/resp"
)

// execute는 연결이 해석한 RESP 배열에서 첫 원소를 명령 이름으로, 나머지를 인자로 읽는다.
func execute(command resp.Value) resp.Value {
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
