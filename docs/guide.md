# GoRedis

Go로 Redis를 구현하는 학습 프로젝트입니다. 각 TCP 연결에서 RESP2 요청을 읽습니다. 단독 서버는 하나의 실행기에서 명령을 처리하고, 클러스터는 키의 해시 슬롯에 따라 여러 primary에서 명령을 나누어 처리합니다. `PING`, `SET`, `GET`, `TYPE`, `INCR`, `MGET`, `MSET`, `HSET`, `HGET`, `DEL`, `EXISTS`, `EXPIRE`, `TTL`을 지원합니다.

## 네 계층을 따라 코드 읽기

진입점 `main`은 실행 옵션을 읽고 단독 primary, 읽기 전용 replica(`-replicaof`), 클러스터 노드(`-cluster-config`) 중 한 경로를 선택합니다. 옵션 없이 시작하면 단독 primary입니다. [main.go](../cmd/goredis/main.go#L14)

| 계층 | 코드에서 볼 동작 | 먼저 열 파일 |
| --- | --- | --- |
| ① 접근 | TCP 연결을 수락하고 연결마다 고루틴을 시작합니다. 각 고루틴은 요청을 RESP2 값으로 읽고 응답을 같은 소켓에 씁니다. | [server.go](../internal/server/server.go#L118) → [reader.go](../internal/resp/reader.go#L30) → [writer.go](../internal/resp/writer.go#L22) |
| ② 실행 | 단독 서버에서는 모든 연결이 같은 실행 고루틴에 명령을 보냅니다. 실행기가 명령을 선택하고 `Store`의 값과 만료 시각을 읽거나 바꿉니다. | [engine.go](../internal/engine/engine.go#L69) → [command.go](../internal/engine/command.go#L15) → [store.go](../internal/store/store.go#L12) |
| ③ 보존 | 단독 primary는 변경을 증분 AOF에 기록합니다. 재시작하면 manifest가 가리키는 RDB 기준본을 읽고 그 뒤의 증분 파일을 순서대로 재생합니다. | [aof.go](../internal/persist/aof.go#L332) → [rewrite.go](../internal/persist/rewrite.go#L66) → [multipart.go](../internal/persist/multipart.go#L80) |
| ④ 분산 | `-replicaof`는 primary의 스냅샷과 이후 변경을 비동기로 받습니다. 클러스터는 키의 슬롯으로 샤드를 고르고 샤드별 Raft 그룹이 복제와 primary 선출을 담당합니다. | [stream.go](../internal/replica/stream.go#L14) · [follow.go](../internal/replica/follow.go#L17) / [route.go](../internal/cluster/route.go#L83) → [group.go](../internal/cluster/group.go#L123) → [raft_fsm.go](../internal/cluster/raft_fsm.go#L46) |

그림의 ‘이벤트 루프’에 해당하는 부분은 GoRedis에서 연결을 받는 `Accept` 루프와 연결별 `handleConn` 루프로 나뉩니다. ‘단일 스레드 실행’은 단독 서버의 키 공간을 고루틴 하나가 소유한다는 뜻입니다. 연결 A가 다음 요청을 기다리는 동안 연결 B도 요청을 읽을 수 있고, 두 연결의 명령이 저장소에 도착하면 실행기가 하나씩 처리합니다. [연결 수락](../internal/server/server.go#L132) · [명령 전달](../internal/engine/engine.go#L97)

### 단독 서버에서 SET과 GET 따라가기

1. 클라이언트 A가 `SET name alice EX 60`을 보냅니다. `handleConn`의 `ReadValue`가 다섯 벌크 문자열을 담은 배열을 읽습니다. `Execute`는 이 요청을 `requests` 채널에 넘기고 결과를 기다립니다. [handleConn](../internal/server/server.go#L145) · [Execute](../internal/engine/engine.go#L112)
2. 실행기가 `SET` 분기에서 인자 수와 쓰기 권한을 검사합니다. `set`은 실행 시각에 60초를 더해 절대 만료 시각을 한 번 정합니다. [명령 분기](../internal/engine/command.go#L51) · [set](../internal/engine/set.go#L10)
3. 기본 영속화 설정에서 `writeString`은 먼저 증분 AOF에 `SET name alice PXAT <만료 시각(ms)>`을 기록합니다. 기록에 성공하면 메모리에 값 `alice`와 같은 만료 시각을 저장하고, 복제 구독자가 있으면 변경을 큐에 넣습니다. 연결 고루틴은 `+OK\r\n`을 보냅니다. 기본 `everysec`에서는 운영체제에 기록한 후 백그라운드에서 디스크를 동기화하고, `always`에서는 디스크 동기화까지 마쳐야 쓰기가 성공합니다. [writeString](../internal/engine/set.go#L56) · [AOF 기록](../internal/persist/aof.go#L332) · [fsync 정책](../internal/persist/sync.go#L71)
4. 클라이언트 B가 같은 서버에 `GET name`을 보내면 같은 실행기의 `StringAt` 호출이 메모리를 조회합니다. 만료 전에는 벌크 문자열 `alice`를 돌려줍니다. 만료 시각이 지났다면 `lookup`이 값을 제거하고 null 응답을 보냅니다. [GET 분기](../internal/engine/command.go) · [lookup](../internal/store/store.go#L169) · [응답 전송](../internal/server/server.go#L187)

### 클러스터에서 달라지는 경로

클러스터 노드의 `handleConn`은 같은 RESP 요청을 `Cluster.Execute`로 넘깁니다. `route`가 키의 슬롯을 계산하고, 다른 primary가 담당하면 `MOVED`와 주소를 반환합니다. 담당 primary에서는 `group.execute`가 명령을 Raft 로그에 제출합니다. 과반수가 기록해 커밋되면 각 노드의 `replicatedFSM.Apply`가 자기 메모리에 같은 순서로 명령을 적용합니다. 이 경로는 `GET`에도 적용됩니다. [슬롯 라우팅](../internal/cluster/route.go#L83) · [로그 제출](../internal/cluster/group.go#L123) · [샤드 실행기](../internal/engine/cluster.go#L21)

그림의 Sentinel에 해당하는 독립적인 감시 프로세스는 구현되어 있지 않습니다. 단독 replica는 재접속해 지정된 primary를 계속 따릅니다. 장애 시 자동 승격은 클러스터 모드에서 같은 샤드의 Raft 투표자들이 새 primary를 선출할 때 일어납니다. [단독 replica 재연결](../internal/replica/follow.go#L17) · [Raft 그룹 구성](../internal/cluster/group.go#L26)

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

`EXPIRE name 1`은 기존 키에 1초 만료를 설정하고 정수 1을 반환합니다. 키가 없으면 0을 반환합니다. 만료 시각부터 모든 연결의 `GET name`은 null을 반환합니다. `TTL name`은 남은 시간(초), 만료 설정이 없는 키에는 -1, 없는 키에는 -2를 반환합니다. 같은 키를 옵션 없는 `SET`으로 덮어쓰면 기존 만료 설정이 해제됩니다.

`SET key value EX seconds`는 값과 만료 시각을 한 명령으로 설정합니다. `NX`를 함께 쓰면 키가 없거나 이미 만료됐을 때만 저장합니다. `NX`를 지정했는데 기존 키가 있으면 null을 반환하고 원래 값과 만료 시각을 보존합니다. 다음은 빈 저장소에서 순서대로 실행한 예입니다.

```text
SET session alice EX 60 NX   → OK
SET session bob NX EX 300    → (nil)
GET session                  → "alice"
EXISTS session session other → 2
DEL session session other    → 1
EXISTS session               → 0
```

`EXISTS`는 인자로 받은 키를 하나씩 세므로 같은 키를 두 번 주면 두 번 셉니다. `DEL`은 실제로 삭제한 키 수를 반환합니다. 두 명령 모두 만료된 키를 없는 키로 처리합니다. `EX`에는 양의 정수 초를 지정하며, 계산한 절대 만료 시각이 64비트 밀리초 범위를 벗어나면 오류를 반환합니다.

### 카운터와 여러 키 읽기·쓰기

```text
MSET visits 9 owner alice       → OK
INCR visits                     → 10
TYPE visits                     → string
MGET owner missing visits owner → ["alice", (nil), "10", "alice"]
TYPE missing                    → none
```

`INCR`는 없는 키를 0에서 시작하고, 기존 키를 증가시키면 그 키의 만료 시각을 유지합니다. 정수 형식이 잘못됐거나 증가 결과가 부호 있는 64비트 범위를 벗어나면 오류를 반환하고 값을 보존합니다.

`MSET`은 키·값 쌍을 한 명령으로 저장하며 덮어쓴 키의 TTL을 해제합니다. 같은 키가 여러 번 나오면 마지막 값이 남습니다. 다른 클라이언트의 `MGET`은 한 시점의 값을 인자 순서대로 돌려줍니다.

### 해시의 필드 읽기·쓰기

아래 명령은 하나의 키에 이름과 도시를 저장합니다. `HSET`은 새로 추가한 필드 수를 반환합니다. 한 요청에서 같은 필드가 반복되면 마지막 값을 저장하고, 그 필드가 새로 생겼을 때만 한 번 셉니다.

```text
HSET user:42 name alice city Seoul → 2
HSET user:42 city Busan            → 0
HGET user:42 city                  → "Busan"
HGET user:42 missing               → (nil)
TYPE user:42                       → hash
```

`EXPIRE user:42 60`을 실행한 뒤 `HSET`으로 필드를 수정해도 키의 만료 시각은 유지됩니다. 키가 만료되면 모든 필드의 `HGET`이 null을 반환합니다. 문자열 키에 `HGET`이나 `HSET`을 보내면 `WRONGTYPE` 오류가 발생합니다. 해시 키를 `MGET`에 섞으면 결과 배열의 해당 자리만 null입니다.

## RDB 스냅샷과 증분 AOF로 복구하기

단독 서버의 기본 영속화는 Redis 7 이후의 다중 파일 AOF 방식입니다. Redis에서 대응하는 설정은 `appendonly yes`와 `aof-use-rdb-preamble yes`입니다. GoRedis가 복구하는 데이터는 DB 0의 문자열, 해시와 키 단위 TTL입니다. 다음은 스냅샷 재작성이 한 번 끝난 디렉터리의 예입니다. [Redis 영속화 문서](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/)

```text
appendonlydir/
├── appendonly.aof.2.base.rdb   # 스냅샷 시점의 문자열, 해시, 키별 절대 만료 시각
├── appendonly.aof.2.incr.aof   # 그 시점 이후의 쓰기를 실행 순서대로 기록한 RESP 명령
└── appendonly.aof.manifest     # 복구할 base와 증분 파일 목록
```

1. **새 스냅샷을 시작할 때** 단일 실행기가 한 명령을 마친 시점에 키 공간을 복사하고 증분 AOF를 새 파일로 전환합니다. 이후 클라이언트의 쓰기는 새 파일에 쌓이고, 별도 고루틴이 복사본을 RDB로 기록합니다. 복사하는 동안에는 다른 명령 실행이 잠시 대기합니다.
2. **스냅샷을 쓰는 동안** manifest는 이전 RDB와 필요한 모든 증분 파일을 가리킵니다. 새 RDB의 디스크 동기화가 끝나면 manifest를 원자적으로 교체합니다. 이때부터 새 RDB와 그 이후의 증분 파일만 복구에 필요합니다.
3. **재시작할 때** manifest에서 지정한 RDB를 읽고 증분 파일들을 순서대로 적용합니다. 마지막으로 현재 시각을 기준으로 만료된 키를 정리합니다. 예전 기한이 지났어도 이후 `EXPIRE`가 기한을 연장한 키는 이 순서 덕분에 살아납니다. 증분 파일 끝에 쓰다 만 명령이 있으면 완전한 명령까지만 복구합니다. [복구 코드](../internal/persist/multipart.go) · [RDB 형식](../internal/persist/rdb.go) · [증분 기록 재생](../internal/persist/aof.go)

기본 설정에서는 AOF 전체 크기가 64 MiB를 넘고 재작성 기준 크기의 두 배 이상이 되면 자동으로 새 스냅샷을 만듭니다. 수동으로는 `BGREWRITEAOF`를 사용합니다. 응답은 백그라운드 작업의 시작을 뜻합니다. 아래 예제에서 서버를 종료하고 같은 경로로 재시작하면 `GET visits`는 `"2"`를 반환합니다. [BGREWRITEAOF 명령](https://redis.io/docs/latest/commands/bgrewriteaof/)

```sh
redis-cli SET visits 1
redis-cli BGREWRITEAOF
redis-cli INCR visits
```

기본 `-appendfsync everysec`은 증분 기록을 운영체제에 쓰고 약 1초마다 백그라운드에서 디스크에 동기화합니다. 전원 장애가 나면 아직 동기화되지 않은 최근 쓰기가 유실될 수 있습니다. 이전 GoRedis처럼 매 쓰기 응답 전에 동기화하려면 `-appendfsync always`를, 운영체제에 동기화를 맡기려면 `-appendfsync no`를 사용합니다.

기본 파일은 서버를 실행한 위치의 `appendonlydir/`에 생깁니다. 이전 버전의 단일 `appendonly.aof`만 있으면 처음 시작할 때 읽어 RDB 기준본을 만들고 원본 파일은 보관합니다. 그 뒤에는 manifest를 기준으로 복구합니다. `-aof /data/app.aof`처럼 이름을 직접 지정하면 `/data/app.aof.d/`에 저장합니다. 재시작 시 같은 경로를 사용하며, `-aof=""`는 메모리 전용 모드입니다.
