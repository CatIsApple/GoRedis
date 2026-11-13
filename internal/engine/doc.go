// Package engine은 클라이언트 명령과 복제 작업을 하나의 실행 고루틴으로 모은다.
// engine.go는 작업 전달, command.go는 명령 실행, replication.go는 스냅샷과 변경 구독을 담당한다.
package engine
