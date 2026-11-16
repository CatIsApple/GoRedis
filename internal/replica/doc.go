// Package replica는 GoRedis 노드끼리 키 공간과 그 이후의 변경을 RESP2로 전송한다.
// replica가 GOREDIS.SYNC를 보내면 primary는 FULLSYNC, 스냅샷의 각 키, READY 순서로
// 응답한다. 그 뒤 같은 연결에서 실행 순서대로 변경을 보내고, 재연결 시 전체 동기화를 다시 한다.
package replica
