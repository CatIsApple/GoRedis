  ## Directory structure

```text
GoRedis/
├── cmd/goredis/       main.go에서 실행 옵션과 종료 신호를 설정
├── internal/
│   ├── server/        TCP 연결 수락, 요청을 실행기에 전달, 응답 전송
│   ├── resp/          TCP와 AOF에서 공통으로 쓰는 RESP2 값·리더·라이터
│   ├── engine/        단독 서버와 클러스터 샤드의 명령 실행
│   ├── store/         문자열·해시와 키의 절대 만료 시각을 메모리에 보관
│   ├── persist/       RDB 스냅샷, 증분 AOF 기록·재생, manifest 교체
│   ├── replica/       단독 primary의 스냅샷 전송, 이후 변경 수신, 재연결
│   └── cluster/       해시 슬롯 라우팅, 샤드별 Raft 복제와 primary 선출
├── examples/cluster.json  로컬 클러스터의 샤드와 노드 주소
├── docs/guide.md      계층별 코드 읽기와 실행 예제
├── go.mod
├── go.sum
└── README.md
```

[계층별 코드 읽기와 실행 예제](docs/guide.md)
