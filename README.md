# GoRedis

Go로 Redis를 구현하는 학습 프로젝트입니다. TCP 서버가 연결마다 고루틴을 열어 클라이언트가 보낸 바이트를 읽습니다.

## 개발 환경

Go 1.26.0 이상에서 저장소 루트를 기준으로 실행합니다.

```sh
go run ./cmd/goredis   # 127.0.0.1:6379에서 실행
go build ./cmd/goredis # 루트에 goredis 실행 파일 생성
go vet ./...           # 정적 검사
```
