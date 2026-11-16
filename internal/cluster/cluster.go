package cluster

import (
	"context"
	"io"
	"net"
	"path/filepath"
	"sync"
	"time"

	"github.com/CatIsApple/GoRedis/internal/resp"
)

type Cluster struct {
	*topology
	group *group

	mu     sync.RWMutex
	owners []string
	cancel context.CancelFunc
	done   chan struct{}
}

func Open(ctx context.Context, addr, configPath, dir string) (*Cluster, error) {
	layout, err := readConfig(configPath, addr)
	if err != nil {
		return nil, err
	}
	if dir == "" {
		// nodeID는 클라이언트 주소의 고정 길이 16진수 ID다. IPv6 주소와
		// Windows 경로에서도 포트별 상태 디렉터리를 안전하게 구별한다.
		dir = filepath.Join("cluster-data", nodeID(addr))
	}
	g, err := openGroup(layout.config.Shards[layout.shard], layout.local, dir, layout.topologyID)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	c := &Cluster{
		topology: layout,
		group:    g,
		owners:   make([]string, len(layout.config.Shards)),
		cancel:   cancel,
		done:     make(chan struct{}),
	}
	go c.discover(ctx)
	return c, nil
}

func (c *Cluster) Close() error {
	c.cancel()
	<-c.done
	return c.group.close()
}

func (c *Cluster) owner(shard int) string {
	if shard == c.shard {
		return c.group.leader()
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.owners[shard]
}

// 원격 노드에는 라우팅에 쓸 주소만 묻는다. 이 응답을 받더라도 쓸 권한은
// 얻지 못한다. 실제 키 명령은 담당 그룹의 과반수 커밋을 별도로 거친다.
func (c *Cluster) leaderReply(args []string) resp.Value {
	if len(args) != 2 || args[1] != c.topologyID {
		return resp.Value{Kind: resp.SimpleError, Bytes: []byte("ERR cluster topology ID required")}
	}
	var address []byte
	if c.group.leader() == c.local.Addr {
		address = []byte(c.local.Addr)
	}
	return resp.Value{Kind: resp.BulkString, Bytes: address}
}

func (c *Cluster) discover(ctx context.Context) {
	defer close(c.done)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for ctx.Err() == nil {
		// 각 샤드는 독립적으로 소유자를 찾는다. 멈춘 샤드의 타임아웃이
		// 정상 샤드의 주소 갱신이나 사용자 요청을 막지 않는다.
		var wg sync.WaitGroup
		for i := range c.config.Shards {
			if i != c.shard {
				wg.Go(func() { c.discoverShard(ctx, i) })
			}
		}
		wg.Wait()
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (c *Cluster) discoverShard(ctx context.Context, i int) {
	nodes := c.config.Shards[i].Nodes
	previous := c.owner(i)
	// 정상 상태라면 기존 primary 한 곳에만 묻는다. 응답이 끊기거나
	// 강등된 경우에만 나머지 투표자를 병렬로 확인한다.
	if previous != "" && c.reportsLeader(ctx, previous) {
		return
	}
	found := make(chan string, len(nodes))
	var wg sync.WaitGroup
	for _, node := range nodes {
		if node.Addr != previous {
			wg.Go(func() {
				if c.reportsLeader(ctx, node.Addr) {
					found <- node.Addr
				}
			})
		}
	}
	wg.Wait()
	close(found)
	owner := ""
	for addr := range found {
		if owner != "" {
			// 임기가 교체되는 도중의 상충된 관측은 다음 주기에서 다시 묻는다.
			owner = ""
			break
		}
		owner = addr
	}
	c.mu.Lock()
	c.owners[i] = owner
	c.mu.Unlock()
}

func (c *Cluster) reportsLeader(ctx context.Context, addr string) bool {
	const timeout = 350 * time.Millisecond
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return false
	}
	defer conn.Close()
	deadline, _ := ctx.Deadline()
	if err := conn.SetDeadline(deadline); err != nil {
		return false
	}
	w := resp.NewWriter(conn)
	frame := resp.Value{Kind: resp.Array, Elements: []resp.Value{
		{Kind: resp.BulkString, Bytes: []byte("GOREDIS.LEADER")},
		{Kind: resp.BulkString, Bytes: []byte(c.topologyID)},
	}}
	if err := w.WriteValue(frame); err != nil {
		return false
	}
	if err := w.Flush(); err != nil {
		return false
	}
	v, err := resp.NewReader(io.LimitReader(conn, 4096)).ReadValue()
	return err == nil && v.Kind == resp.BulkString && string(v.Bytes) == addr
}
