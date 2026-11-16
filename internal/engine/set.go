package engine

import (
	"github.com/CatIsApple/GoRedis/internal/resp"
	"github.com/CatIsApple/GoRedis/internal/store"
)

// SET은 옵션을 모두 해석한 뒤 조건을 검사한다. 따라서 이미 키가 있는 경우에도
// 잘못된 EX 인자를 주면 NX의 null 응답 대신 문법 또는 만료 시간 오류를 돌려준다.
func (s *state) set(args []resp.Value, now int64) resp.Value {
	var nx bool
	var expiry []byte
	for i := 3; i < len(args); i++ {
		option := args[i].Bytes
		// Redis는 두 글자 옵션 바로 다음의 NUL을 옵션 이름의 끝으로 인식한다.
		if len(option) > 2 && option[2] == 0 {
			option = option[:2]
		}
		switch {
		case commandIs(option, "NX"):
			nx = true
		case commandIs(option, "EX") && i+1 < len(args):
			i++
			// Redis는 EX가 반복되면 마지막 EX의 초 값만 검사해 사용한다.
			expiry = args[i].Bytes
		default:
			return commandError("ERR syntax error")
		}
	}

	var deadline int64
	if expiry != nil {
		seconds, ok := parseInt(string(expiry))
		if !ok {
			return commandError(integerError)
		}
		var err error
		deadline, err = store.ExpiryDeadline(now, seconds)
		if err != nil || deadline == 0 {
			return commandError("ERR invalid expire time in 'set' command")
		}
	}

	key := string(args[1].Bytes)
	if nx && s.db.ExistsAt(key, now) {
		return resp.Value{Kind: resp.BulkString}
	}
	if err := s.writeString(key, string(args[2].Bytes), deadline); err != nil {
		return commandError(journalError)
	}
	return resp.Value{Kind: resp.SimpleString, Bytes: []byte("OK")}
}

// writeString은 문자열의 최종 값과 절대 기한을 함께 기록한다. SET과 INCR 모두
// AOF 기록에 성공한 뒤에만 메모리와 복제 구독자에게 같은 값을 반영한다.
func (s *state) writeString(key, value string, deadline int64) error {
	if s.journal != nil {
		var err error
		if deadline != 0 {
			// 값과 절대 기한을 한 레코드로 기록해야 중간 종료로 TTL만 유실되지 않는다.
			err = s.journal.AppendSetExpireAt(key, value, deadline)
		} else {
			err = s.journal.AppendSet(key, value)
		}
		if err != nil {
			return err
		}
	}

	s.db.Set(key, value)
	if deadline != 0 {
		s.db.SetExpiryAt(key, deadline)
	}
	s.publish(key)
	return nil
}
