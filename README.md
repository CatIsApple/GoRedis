# GoRedis

Go로 Redis를 구현하는 학습 프로젝트입니다. 각 TCP 연결에서 RESP2 요청을 읽고 서버의 단일 실행기에서 명령을 하나씩 실행합니다. `PING [message]`, `SET key value`, `GET key`를 지원합니다.

## 개발 환경

Go 1.26.0 이상에서 저장소 루트를 기준으로 실행합니다.

```sh
go run ./cmd/goredis   # 127.0.0.1:6379에서 실행
go build ./cmd/goredis # 루트에 goredis 실행 파일 생성
go vet ./...           # 정적 검사
```

서버 실행 중 다른 터미널에서 아래처럼 RESP2로 인코딩한 `PING`을 보낼 수 있습니다. 서버 터미널에서 Ctrl+C를 누르면 리스너와 열린 연결을 모두 닫습니다. 포트를 바꾸려면 `go run ./cmd/goredis -addr 127.0.0.1:6380`으로 실행합니다.

```sh
printf '*1\r\n$4\r\nPING\r\n' | nc -w 1 127.0.0.1 6379
# +PONG
```

아래 두 명령은 각각 새 TCP 연결을 엽니다. 첫 번째 연결에서 저장한 값을 두 번째 연결에서 읽을 수 있습니다.

```sh
printf '*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$5\r\nalice\r\n' | nc -w 1 127.0.0.1 6379
# +OK
printf '*2\r\n$3\r\nGET\r\n$4\r\nname\r\n' | nc -w 1 127.0.0.1 6379
# $5
# alice
```
