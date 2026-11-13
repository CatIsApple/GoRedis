# GoRedis

Go로 Redis를 구현하는 학습 프로젝트입니다. 각 TCP 연결에서 RESP2 메시지를 읽고 씁니다. 실행기를 연결하기 전까지 요청에 오류 응답을 보냅니다.

## 개발 환경

Go 1.26.0 이상에서 저장소 루트를 기준으로 실행합니다.

```sh
go run ./cmd/goredis   # 127.0.0.1:6379에서 실행
go build ./cmd/goredis # 루트에 goredis 실행 파일 생성
go vet ./...           # 정적 검사
```
