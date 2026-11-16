package engine

import (
	"github.com/CatIsApple/GoRedis/internal/resp"
	"github.com/CatIsApple/GoRedis/internal/store"
)

func (s *state) hset(key string, args []resp.Value, now int64) resp.Value {
	kind := s.db.TypeAt(key, now)
	create := kind == "none"
	if !create && kind != "hash" {
		return commandError(store.ErrWrongType.Error())
	}
	fields := parsePairs(args)
	// 만료된 키를 재사용하면 새 해시가 된다. 이 구분을 기록해야 재시작이나
	// 복제 때 이전 해시의 필드와 TTL이 새 키에 섞이지 않는다.
	if s.journal != nil && s.journal.AppendHSet(key, fields, create) != nil {
		return commandError(journalError)
	}
	added := s.db.HSet(key, fields)
	s.publishHash(key, fields, create, s.db.ExpiresAt(key))
	return resp.Value{Kind: resp.Integer, Int: added}
}
