package resp

import "errors"

// Kind는 RESP2 값의 첫 바이트로 자료형을 구분한다.
type Kind byte

const (
	SimpleString Kind = '+'
	SimpleError  Kind = '-'
	Integer      Kind = ':'
	BulkString   Kind = '$'
	Array        Kind = '*'
)

// Value에서 문자열은 Bytes, 정수는 Int, 배열은 Elements에 담긴다.
// BulkString과 Array에서 nil은 null이고, 길이 0의 비-nil 슬라이스는 빈 값이다.
// 예를 들어 GET에서 키가 없으면 Bytes가 nil인 BulkString($-1\r\n),
// 저장된 값이 빈 문자열이면 Bytes가 []byte{}인 BulkString($0\r\n\r\n)이다.
type Value struct {
	Kind     Kind
	Bytes    []byte
	Int      int64
	Elements []Value
}

// ErrProtocol은 RESP 형식 또는 이 구현의 크기 제한을 위반했음을 나타낸다.
var ErrProtocol = errors.New("RESP protocol error")

// 길이 헤더나 배열 중첩 하나로 요구할 수 있는 메모리와 재귀 깊이에 상한을 둔다.
const (
	maxLineBytes = 64 << 10
	maxBulkBytes = 512 << 20
	maxArrayLen  = 1 << 20
	maxDepth     = 64
)
