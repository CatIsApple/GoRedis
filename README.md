# GoRedis

Go로 Redis를 구현하는 학습 프로젝트입니다. 각 TCP 연결에서 RESP2 요청을 읽고 서버의 단일 실행기에서 명령을 하나씩 실행합니다. `PING [message]`, `SET key value`, `GET key`, `EXPIRE key seconds`, `TTL key`를 지원합니다.

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

`EXPIRE name 1`은 기존 키에 1초 만료를 설정하고 정수 1을 반환합니다. 키가 없으면 0을 반환합니다. 만료 시각부터 모든 연결의 `GET name`은 null을 반환합니다. `TTL name`은 남은 시간(초), 만료 설정이 없는 키에는 -1, 없는 키에는 -2를 반환합니다. 같은 키에 다시 `SET`하면 기존 만료 설정이 해제됩니다.

## AOF와 재시작

기본 AOF 경로는 서버를 실행한 디렉터리의 `appendonly.aof`입니다. 다른 디렉터리에서도 같은 데이터를 열려면 `-aof`에 같은 절대 경로를 지정합니다. `-aof=""`는 메모리 전용 모드입니다.

`SET`은 키와 값을 기록합니다. 성공한 `EXPIRE`는 설정 당시 계산한 Unix 밀리초 단위 만료 시각을 `PEXPIREAT` 레코드로 남깁니다. 재시작하면 AOF를 순서대로 재생하고 최종 만료 시각을 확인합니다. 예를 들어 10초 만료를 설정하고 즉시 서버를 3초간 꺼 두면 재시작 직후 약 7초가 남습니다.
