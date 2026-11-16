# GoRedis

Go로 Redis를 구현하는 학습 프로젝트입니다. 각 TCP 연결에서 RESP2 요청을 읽고 서버의 단일 실행기에서 명령을 하나씩 실행합니다. `PING [message]`, `SET key value`, `GET key`, `EXPIRE key seconds`, `TTL key`를 지원합니다.

## 디렉터리 구조

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

### SET 한 건으로 코드 따라가기

1. [서버의 `handleConn`](internal/server/server.go)이 연결에서 [`Reader.ReadValue`](internal/resp/reader.go)를 호출한다. `SET name alice`는 세 개의 벌크 문자열을 담은 배열로 읽힌다.
2. [`Engine.Execute`](internal/engine/engine.go)는 요청 처리를 실행 고루틴에 넘기고 완료를 기다린다. 실행 고루틴은 [`execute`의 SET 분기](internal/engine/command.go)에서 인자 수를 검사한다.
3. AOF를 사용하면 [`AppendSet`](internal/persist/aof.go)으로 변경을 기록하고 디스크 동기화를 기다린다. 이어서 [`Store.Set`](internal/store/store.go)이 값을 저장하고, 이 키에 있던 만료 시각을 지운다.
4. 실행기는 구독 중인 replica의 큐에 변경을 넣고 `OK`를 반환한다. 연결 고루틴이 [`Writer.WriteValue`와 `Flush`](internal/resp/writer.go)로 `+OK\r\n`을 보낸다. primary의 응답 시점과 replica에 값이 반영되는 시점은 다를 수 있다.

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

## Primary와 읽기 전용 replica 실행

각 명령을 별도 터미널에서 실행합니다. primary가 AOF를 소유하고, replica는 메모리에 복사본을 유지합니다.

```sh
go run ./cmd/goredis -addr 127.0.0.1:6379
go run ./cmd/goredis -addr 127.0.0.1:6380 -replicaof 127.0.0.1:6379
```

`redis-cli`가 설치돼 있다면 두 포트의 동작을 확인할 수 있습니다.

```sh
redis-cli -p 6379 SET name alice    # OK
redis-cli -p 6380 GET name          # 복제 반영 후 "alice"
redis-cli -p 6379 EXPIRE name 30    # 1
redis-cli -p 6380 TTL name          # 복제 반영 후 남은 초
redis-cli -p 6380 SET name bob      # READONLY
```

1. replica가 primary에 접속하면 [`Subscribe`](internal/engine/replication.go)가 단일 실행기 안에서 스냅샷 생성과 변경 구독을 함께 처리합니다. 스냅샷을 전송하는 동안 들어온 쓰기는 구독 큐에 쌓입니다.
2. replica는 모든 키를 임시 저장소에 모은 뒤 스냅샷 종료 신호를 받으면 저장소를 한 번에 교체합니다. 첫 동기화가 끝나기 전의 `GET`과 `TTL`은 `LOADING`을 반환합니다. [수신 코드](internal/replica/follow.go)
3. 이후 primary는 변경된 키의 값과 절대 만료 시각을 실행 순서대로 보냅니다. 복제는 비동기이므로 primary가 쓰기를 승인한 직후 replica에서 읽으면 이전 값이 보일 수 있습니다. [전송 코드](internal/replica/stream.go)
4. 연결이 끊기거나 전송이 밀려 구독 큐가 가득 차면 다시 접속해 전체 동기화를 수행합니다. replica는 재연결 중에도 마지막으로 반영한 데이터를 제공하며 각 키의 만료 시각을 적용합니다.

노드 간 통신은 GoRedis 전용 RESP2 명령 `GOREDIS.SYNC`를 사용합니다. Redis 클라이언트로 일반 명령을 보낼 수 있으며, 복제에는 양쪽 모두 GoRedis 노드를 사용합니다. 클러스터 샤딩과 장애 시 자동 승격은 후속 단계입니다.

## 멀티코어 처리량과 분산 확장

현재 `server`는 연결마다 고루틴을 두어 한 클라이언트의 입력을 기다리는 동안 다른 연결의 입력을 처리한다. 이후 단계에서는 다음 기준으로 확장한다.

- **명령 실행** — 연결 고루틴이 RESP 입출력을 처리한다. 실행 고루틴은 모든 연결이 공유하는 채널에서 명령을 받아 하나씩 처리한다. 각 연결의 요청과 응답 순서는 유지된다.
- **키 저장소** — 단일 실행기가 메모리 키 공간의 읽기, 갱신, 만료를 처리한다. 키에 접근할 때 만료를 확인하고, 다시 조회되지 않는 키도 같은 실행기가 주기적으로 정리한다. 키 공간 샤딩은 단일 실행기의 처리량과 지연을 측정한 뒤 검토한다.
- **노드 확장** — 분산 단계에서는 키가 속한 파티션의 담당 노드를 찾는 라우팅과, 해당 노드의 변경을 복제본에 전달하는 처리를 구분한다. 여러 노드에 걸친 명령의 지원 범위와 일관성 보장은 이 단계에서 정의한다.

저장소 명령을 구현한 뒤에는 RESP 요청부터 응답까지의 초당 완료 명령 수와 p99 지연을 측정한다.

### TODO

- [ ] 커널 수준 네트워크 최적화는 명령 처리까지 포함한 부하 테스트에서 시스템 호출이나 소켓 처리가 병목으로 확인될 때 검토한다.
