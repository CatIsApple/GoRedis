  ## Directory structure

```text
GoRedis/
├── cmd/goredis/       main.go에서 실행 옵션과 종료 신호를 설정
├── internal/
│   ├── server/        TCP 연결 수락, 요청을 실행기에 전달, 응답 전송
│   ├── resp/          TCP와 AOF에서 공통으로 쓰는 RESP2 값·리더·라이터
│   ├── engine/        모든 연결이 공유하는 단일 실행기와 명령별 처리
│   ├── store/         키·값과 절대 만료 시각을 메모리에 보관
│   ├── persist/       AOF 파일 열기, 재생, 쓰기, 디스크 동기화
│   └── replica/       primary의 스냅샷 전송, 이후 변경 수신, 재연결
├── go.mod
└── README.md
```
