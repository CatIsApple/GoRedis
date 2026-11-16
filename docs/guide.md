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
