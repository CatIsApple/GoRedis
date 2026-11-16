// Package resp는 TCP 연결이나 AOF에서 읽은 RESP2 바이트를 Value로 바꾸고,
// Value를 같은 형식으로 기록한다.
//
// 예를 들어 SET k hi는 다음 바이트로 표현한다. 여기서 \r\n은 두 바이트의 줄 끝이다.
//
//	*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$2\r\nhi\r\n
//
// *3은 배열 원소 수이고, $3, $1, $2는 바로 뒤에 오는 각 문자열의 바이트 수다.
// Reader는 이 요청을 세 BulkString을 담은 Array로 읽는다. SET의 의미는 engine에서 해석한다.
package resp
