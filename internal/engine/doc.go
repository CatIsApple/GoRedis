// Package engine은 한 키 공간에 들어온 명령을 차례로 실행한다.
// engine.go는 작업 전달, command.go는 명령 분기, set.go는 조건부 저장과 만료 처리를 맡는다.
// strings.go는 카운터와 여러 키 읽기·쓰기, hashes.go는 HSET의 기록과 필드 갱신을 맡는다.
// replication.go는 단독 서버 복제의 스냅샷과 변경 구독을 담당한다.
// 클러스터에서는 cluster.go가 샤드별 Raft 로그와 함께 받은 시각을 명령 실행기에 넘긴다.
package engine
