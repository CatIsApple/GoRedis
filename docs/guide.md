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

## Primary와 읽기 전용 replica 실행

각 명령을 별도 터미널에서 실행합니다. primary가 RDB와 증분 AOF를 소유하고, replica는 메모리에 복사본을 유지합니다.

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

1. replica가 primary에 접속하면 [`Subscribe`](../internal/engine/replication.go)가 단일 실행기 안에서 스냅샷 생성과 변경 구독을 함께 처리합니다. 스냅샷을 전송하는 동안 들어온 쓰기는 구독 큐에 쌓입니다.
2. replica는 모든 키를 임시 저장소에 모은 뒤 스냅샷 종료 신호를 받으면 저장소를 한 번에 교체합니다. 첫 동기화가 끝나기 전에는 `GET`, `MGET`, `HGET`, `TYPE`, `EXISTS`, `TTL`로 키를 조회하면 `LOADING`을 반환합니다. [수신 코드](../internal/replica/follow.go)
3. 이후 primary는 변경된 키의 값과 절대 만료 시각을 실행 순서대로 보냅니다. `MSET`과 다중 `DEL`은 각 명령의 변경을 한 메시지에 담아 replica에서도 한 번에 적용합니다. 기존 해시의 `HSET`은 요청에 포함된 필드만 전송하고 replica가 한 번에 적용합니다. 해시의 만료 시각을 바꾸면 전체 필드와 새 기한을 함께 보내므로, replica에서 이전 기한이 먼저 지나도 값을 복원할 수 있습니다. 복제는 비동기이므로 primary가 쓰기를 승인한 직후 replica에서 읽으면 이전 값이 보일 수 있습니다. [전송 코드](../internal/replica/stream.go)
4. 연결이 끊기거나 전송이 밀려 구독 큐가 가득 차면 다시 접속해 전체 동기화를 수행합니다. replica는 재연결 중에도 마지막으로 반영한 데이터를 제공하며 각 키의 만료 시각을 적용합니다.

노드 간 통신은 GoRedis 전용 RESP2 명령 `GOREDIS.SYNC`를 사용합니다. Redis 클라이언트로 일반 명령을 보낼 수 있으며, 복제에는 양쪽 모두 GoRedis 노드를 사용합니다. 이전 버전의 replica가 지원하지 않는 다중 `MSET` 또는 `DEL` 메시지를 받으면 재연결해 전체 스냅샷을 다시 받습니다. 해시가 포함된 키 공간을 복제하려면 replica에도 해시를 지원하는 버전이 필요합니다.

## 슬롯 분산과 primary 자동 승격

GoRedis는 Redis Cluster와 같은 CRC16/XMODEM 규칙으로 키를 0부터 16,383까지의 슬롯 중 하나에 배정합니다. 예를 들어 `{user:42}:name`과 `{user:42}:city`는 중괄호 안의 `user:42`가 같아서 둘 다 슬롯 15880에 속합니다. 여러 키를 한 명령으로 읽거나 쓰려면 모든 키가 같은 슬롯에 있어야 합니다. [Redis Cluster 명세](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/)

예제 설정은 아래 세 샤드를 만듭니다. 각 행의 노드 세 개가 한 Raft 그룹입니다. 각 그룹을 처음 구성할 때는 설정에 적힌 첫 노드가 투표자 목록을 등록합니다. `addr`는 클라이언트가 접속할 주소이고 `raft`는 같은 샤드 노드끼리 로그와 투표를 교환할 주소입니다. 노드 간 TCP에는 인증과 암호화가 없으므로 여러 호스트에서 실행할 때는 신뢰하는 내부망 주소를 사용합니다. [로컬 설정](../examples/cluster.json)

| 슬롯 범위 | 클라이언트 포트 | Raft 포트 |
| --- | --- | --- |
| 0–5461 | 7100, 7103, 7106 | 17100, 17103, 17106 |
| 5462–10922 | 7101, 7104, 7107 | 17101, 17104, 17107 |
| 10923–16383 | 7102, 7105, 7108 | 17102, 17105, 17108 |

### 로컬에서 실행하고 primary 교체 확인하기

저장소 루트에서 실습 전용 `bash` 셸을 열고 아래 블록을 순서대로 실행합니다. Go, `redis-cli` 7 이상, Python 3을 사용합니다. 새 임시 디렉터리에 노드별 로그와 데이터를 남기므로 실습을 반복할 수 있습니다. `-cluster-dir`을 생략하면 각 노드는 `cluster-data/<노드 ID>/`를 사용합니다.

```bash
bash
```

```bash
run_dir=$(mktemp -d)
go build -o "$run_dir/goredis" ./cmd/goredis
start_node() {
  "$run_dir/goredis" -addr "127.0.0.1:$1" -cluster-config examples/cluster.json \
    -cluster-dir "$run_dir/data-$1" >>"$run_dir/$1.log" 2>&1 &
  echo "$!" >"$run_dir/$1.pid"
}
stop_nodes() {
  for pid in $(jobs -pr); do kill "$pid" 2>/dev/null; done
  wait
}
trap stop_nodes EXIT
for port in {7100..7108}; do start_node "$port"; done
echo "$run_dir"
for attempt in {1..50}; do
  redis-cli --raw -p 7100 CLUSTER INFO 2>/dev/null | grep -q '^cluster_state:ok' && break
  sleep 0.2
done
```

`CLUSTER INFO`에 `cluster_state:ok`가 표시되면 세 샤드의 primary가 준비된 상태입니다. 7100에서 슬롯 15880에 접근하면 `MOVED`에 담당 primary 주소가 나옵니다. `redis-cli -c`는 그 주소로 요청을 다시 보냅니다.

```bash
redis-cli -p 7100 CLUSTER INFO
redis-cli -c -p 7100 SET bar first                              # 슬롯 5061, 첫 샤드
redis-cli -c -p 7100 SET c second                               # 슬롯 7365, 둘째 샤드
redis-cli -p 7100 CLUSTER KEYSLOT '{user:42}:name'              # 15880
redis-cli -p 7100 GET '{user:42}:name'                          # MOVED 15880 ...
redis-cli -c -p 7100 MSET '{user:42}:name' alice '{user:42}:city' Seoul
redis-cli -c -p 7100 MGET '{user:42}:name' '{user:42}:city'    # alice, Seoul
redis-cli -c -p 7100 MSET '{user:42}:name' bob '{user:43}:city' Busan  # CROSSSLOT
```

`CLUSTER SLOTS`의 각 행에는 시작 슬롯, 끝 슬롯, 현재 primary, 설정에 등록된 나머지 노드가 차례로 나옵니다. 이 목록에는 접속이 끊긴 replica도 포함될 수 있습니다. 아래 명령은 슬롯 15880의 현재 primary를 찾아 해당 실습 프로세스만 강제 종료합니다. 선출이 끝나면 기존 값 `alice`가 보이고, 같은 키에 다시 쓸 수 있습니다.

```bash
primary_port() {
  redis-cli -2 --json -p 7100 CLUSTER SLOTS | python3 -c '
import json, sys
print(next(row[2][1] for row in json.load(sys.stdin) if row[0] <= 15880 <= row[1]))'
}
old_port=$(primary_port)
if [[ "$old_port" =~ ^(7102|7105|7108)$ ]]; then
  old_pid=$(cat "$run_dir/$old_port.pid")
  kill -KILL "$old_pid"
  wait "$old_pid" 2>/dev/null
fi
for attempt in {1..50}; do
  value=$(redis-cli --raw -c -p 7100 GET '{user:42}:name' 2>/dev/null)
  [[ "$value" == alice ]] && break
  sleep 0.2
done
echo "기존 primary $old_port, 현재 primary $(primary_port), 복구한 값 $value"
redis-cli -c -p 7100 SET '{user:42}:name' bob
start_node "$old_port"     # 같은 주소와 데이터 디렉터리로 재합류
```

새 primary가 정상인 동안 되돌아온 이전 primary는 replica로 합류해 누락된 로그를 따라잡습니다. 실습 셸에서 `exit`를 실행하면 시작한 서버가 모두 종료됩니다. 전체 재시작을 실험할 때는 셸을 열어 둔 채 `stop_nodes`를 실행하고 앞의 `for port in {7100..7108}; do start_node "$port"; done`을 다시 실행합니다. 각 노드는 기존 디렉터리에서 Raft 스냅샷과 로그를 읽습니다.

### 샤드별 명령 실행과 장애 범위

- 각 샤드의 primary는 명령을 한 순서로 실행합니다. 서로 다른 샤드는 독립적으로 진행합니다. 현재 구현은 `GET` 등 읽기도 샤드의 Raft 로그를 거쳐 커밋하므로 읽기에도 복제 지연과 로그 기록 비용이 듭니다.
- TTL은 커밋된 명령의 시각으로 판단합니다. 클러스터에서 만료된 값의 메모리는 해당 키에 다시 접근하거나 이후 명령 시각으로 스냅샷을 만들 때 회수됩니다.
- 샤드의 과반이 응답하면 명령을 커밋할 수 있습니다. 예제에서 primary 하나가 꺼지면 남은 둘이 새 primary를 선출합니다. 둘이 꺼지면 해당 샤드는 새 요청을 완료할 수 없고, 과반이 살아 있는 다른 샤드는 계속 처리합니다. 명령을 제출한 직후 primary가 바뀌면 해당 클라이언트는 결과 미확정 오류를 받을 수 있습니다. 이때 `INCR`를 그대로 다시 보내면 값이 두 번 증가할 수 있습니다.
- Redis Cluster는 노드 간에 Redis Cluster Bus를 사용합니다. GoRedis 클러스터는 샤드별 Raft를 사용하므로 구성원은 모두 GoRedis로 실행합니다. 슬롯 범위와 투표 구성원은 시작할 때 설정 파일로 정합니다. 온라인 샤드 추가와 슬롯 이동은 지원하지 않습니다. [Redis Cluster 명세](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/)

### TODO

- [ ] 단독 서버와 클러스터에서 RESP 요청부터 응답까지의 초당 완료 명령 수와 p99 지연을 측정한다.
- [ ] 커널 수준 네트워크 최적화는 명령 처리까지 포함한 부하 테스트에서 시스템 호출이나 소켓 처리가 병목으로 확인될 때 검토한다.
