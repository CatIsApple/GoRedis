package cluster

import (
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"

	"github.com/CatIsApple/GoRedis/internal/resp"
	"github.com/hashicorp/raft"
)

const slotCount = 16384

// CRC-16/XMODEM은 초기값 0, 다항식 0x1021을 사용한다. 바이트 하나를
// 처리할 때마다 8번 반복하는 대신 256개 바이트의 계산 결과를 재사용한다.
var crcTable = func() (table [256]uint16) {
	for i := range table {
		crc := uint16(i) << 8
		for range 8 {
			if crc&0x8000 != 0 {
				crc = crc<<1 ^ 0x1021
			} else {
				crc <<= 1
			}
		}
		table[i] = crc
	}
	return table
}()

// KeySlot은 Redis와 같은 해시 태그 규칙을 적용한다. 첫 '{' 뒤 첫 '}'까지
// 비어 있으면 키 전체를 쓴다. 그 뒤에 또 다른 태그가 있어도 다시 찾지 않는다.
func KeySlot(key string) int {
	if open := strings.IndexByte(key, '{'); open >= 0 {
		tail := key[open+1:]
		if end := strings.IndexByte(tail, '}'); end > 0 {
			key = tail[:end]
		}
	}
	var crc uint16
	for i := 0; i < len(key); i++ {
		crc = crc<<8 ^ crcTable[byte(crc>>8)^key[i]]
	}
	return int(crc) % slotCount
}

// slot*n/slotCount로 샤드를 찾는다. 이 식을 역으로 계산하면 샤드 i의
// 첫 슬롯은 ceil(i*slotCount/n)이고 마지막은 다음 샤드의 첫 슬롯 바로 전이다.
func firstSlot(i, n int) int {
	return (i*slotCount + n - 1) / n
}

// 등록된 키 명령은 모두 첫 인자가 키다. arity가 음수면 최소 인자 수,
// keyStep이 0이면 키 한 개, 양수면 다음 키까지의 인자 간격을 뜻한다.
type commandSpec struct{ arity, keyStep int }

var keyCommands = map[string]commandSpec{
	"GET": {2, 0}, "SET": {-3, 0}, "TYPE": {2, 0}, "TTL": {2, 0},
	"INCR": {2, 0}, "EXPIRE": {3, 0}, "HGET": {3, 0}, "HSET": {-4, 0},
	"DEL": {-2, 1}, "EXISTS": {-2, 1}, "MGET": {-2, 1}, "MSET": {-3, 2},
}

// Execute는 연결에서 받은 한 명령의 담당 샤드를 확인한다. MOVED를 받은
// 클라이언트가 해당 primary에 다시 접속하며, 이 노드는 명령을 대리 실행하지 않는다.
func (c *Cluster) Execute(command resp.Value) (resp.Value, bool) {
	if command.Kind != resp.Array || len(command.Elements) == 0 {
		return routeError("ERR expected a non-empty command array"), true
	}
	args := make([]string, len(command.Elements))
	for i, arg := range command.Elements {
		if arg.Kind != resp.BulkString || arg.Bytes == nil {
			return routeError("ERR expected a non-null bulk string"), true
		}
		args[i] = string(arg.Bytes)
	}
	return c.route(args), true
}

func (c *Cluster) route(args []string) resp.Value {
	name := commandName(args[0])
	switch name {
	case "GOREDIS.LEADER":
		return c.leaderReply(args)
	case "PING":
		switch len(args) {
		case 1:
			return resp.Value{Kind: resp.SimpleString, Bytes: []byte("PONG")}
		case 2:
			return bulkString(args[1])
		}
		return arityError(name)
	case "BGREWRITEAOF":
		if len(args) != 1 {
			return arityError(name)
		}
		return routeError("ERR AOF persistence is disabled in cluster mode")
	}

	spec, ok := keyCommands[name]
	if !ok {
		return routeError("ERR unknown command")
	}
	n := len(args)
	if (spec.arity > 0 && n != spec.arity) || (spec.arity < 0 && n < -spec.arity) ||
		(name == "MSET" && n%2 == 0) || (name == "HSET" && n%2 != 0) {
		return arityError(name)
	}
	slot := KeySlot(args[1])
	// MSET은 키와 값을 번갈아 받는다. 값을 해싱하거나 첫 키만 확인하면
	// 서로 다른 슬롯의 쓰기를 잘못된 샤드에서 수행할 수 있다.
	if spec.keyStep > 0 {
		for i := 1 + spec.keyStep; i < n; i += spec.keyStep {
			if KeySlot(args[i]) != slot {
				return routeError("CROSSSLOT Keys in request don't hash to the same slot")
			}
		}
	}
	shard := slot * len(c.config.Shards) / slotCount
	owner := c.owner(shard)
	if shard != c.shard || owner != c.local.Addr {
		return c.redirect(slot, owner)
	}
	reply, err := c.group.execute(args)
	switch {
	case err == nil:
		return reply
	case errors.Is(err, errNotPrimary):
		return c.redirect(slot, c.owner(shard))
	case errors.Is(err, raft.ErrLeadershipLost), errors.Is(err, raft.ErrRaftShutdown):
		// 커밋 직후 승격이 일어났다면 INCR 같은 명령을 재전송할 때 두 번
		// 적용될 수 있다. MOVED는 아직 제출되지 않은 명령에만 돌려준다.
		return routeError("ERR command outcome unknown after leadership change")
	default:
		return routeError("CLUSTERDOWN Shard is unavailable")
	}
}

func (c *Cluster) redirect(slot int, owner string) resp.Value {
	if owner == "" || owner == c.local.Addr {
		return routeError("CLUSTERDOWN Hash slot not served")
	}
	return routeError("MOVED " + strconv.Itoa(slot) + " " + owner)
}

func nodeID(addr string) string {
	id := sha1.Sum([]byte(addr))
	return hex.EncodeToString(id[:])
}

func commandName(raw string) string {
	// 명령 이름만 ASCII 대소문자를 접는다. 키와 값은 이 변환을 통과하지 않는다.
	for i := 0; i < len(raw); i++ {
		if raw[i] >= 0x80 {
			return ""
		}
	}
	return strings.ToUpper(raw)
}

func bulkString(s string) resp.Value { return resp.Value{Kind: resp.BulkString, Bytes: []byte(s)} }
func routeError(s string) resp.Value { return resp.Value{Kind: resp.SimpleError, Bytes: []byte(s)} }
func arityError(name string) resp.Value {
	return routeError("ERR wrong number of arguments for '" + strings.ToLower(name) + "' command")
}
