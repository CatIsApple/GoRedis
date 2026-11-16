package cluster

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/CatIsApple/GoRedis/internal/engine"
	"github.com/CatIsApple/GoRedis/internal/resp"
	"github.com/hashicorp/raft"
	raftbolt "github.com/hashicorp/raft-boltdb/v2"
	"go.etcd.io/bbolt"
)

// group 하나가 담당 슬롯 범위의 로그, 선거, 키 공간을 소유한다.
// Raft ServerID는 클라이언트 주소이며, Raft ServerAddress는 별도 노드 간 포트다.
type group struct {
	node *raft.Raft
	disk *raftbolt.BoltStore
}

var errNotPrimary = raft.ErrNotLeader

func openGroup(shard Shard, local Node, dir, topologyID string) (_ *group, err error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	disk, err := raftbolt.New(raftbolt.Options{
		Path:        filepath.Join(dir, "raft.db"),
		BoltOptions: &bbolt.Options{Timeout: time.Second},
	})
	if err != nil {
		return nil, fmt.Errorf("open raft log: %w", err)
	}
	defer func() {
		if err != nil {
			disk.Close()
		}
	}()

	snaps, err := raft.NewFileSnapshotStore(dir, 2, os.Stderr)
	if err != nil {
		return nil, err
	}
	hadState, err := raft.HasExistingState(disk, disk, snaps)
	if err != nil {
		return nil, err
	}
	if err := bindDataDirectory(disk, local.Addr, topologyID, hadState); err != nil {
		return nil, err
	}

	transport, err := openTransport(local.Raft, topologyID, shard.Nodes[0].Addr)
	if err != nil {
		return nil, fmt.Errorf("listen on raft address %s: %w", local.Raft, err)
	}
	defer func() {
		if err != nil {
			transport.Close()
		}
	}()
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(local.Addr)
	config.LogLevel = "WARN"

	// 최초에는 설정에 나온 첫 노드만 전체 투표자 명단을 기록한다. 복구 시에는
	// 각 노드가 디스크의 임기와 투표 기록을 사용하므로 기존 primary도 선거를 거친다.
	if !hadState && shard.Nodes[0].Addr == local.Addr {
		var peers raft.Configuration
		for _, member := range shard.Nodes {
			peers.Servers = append(peers.Servers, raft.Server{
				ID: raft.ServerID(member.Addr), Address: raft.ServerAddress(member.Raft), Suffrage: raft.Voter,
			})
		}
		if err := raft.BootstrapCluster(config, disk, disk, snaps, transport, peers); err != nil {
			return nil, fmt.Errorf("bootstrap shard: %w", err)
		}
	}

	machine := &replicatedFSM{machine: engine.NewClusterMachine()}
	node, err := raft.NewRaft(config, machine, disk, disk, snaps, transport)
	if err != nil {
		return nil, fmt.Errorf("start raft: %w", err)
	}
	return &group{node: node, disk: disk}, nil
}

// 샤드 순서나 멤버 주소가 바뀌면 같은 디스크 위의 키가 다른 슬롯에 속할 수 있다.
// 로그와 같은 저장소에 최초 설정을 고정해 다른 구성으로 재사용하는 일을 막는다.
func bindDataDirectory(disk *raftbolt.BoltStore, addr, topologyID string, hadState bool) error {
	key := []byte("GoRedisClusterIdentity")
	want := topologyID + "\x00" + addr
	got, err := disk.Get(key)
	if errors.Is(err, raftbolt.ErrKeyNotFound) {
		if hadState {
			return errors.New("raft data directory contains state without cluster identity")
		}
		return disk.Set(key, []byte(want))
	}
	if err != nil {
		return err
	}
	if string(got) != want {
		return errors.New("raft data directory belongs to a different node or cluster topology")
	}
	return nil
}

func (g *group) close() error {
	// Shutdown의 완료까지 기다리면 Raft의 고루틴과 transport가 먼저 종료된다.
	return errors.Join(g.node.Shutdown().Error(), g.disk.Close())
}

func (g *group) leader() string {
	_, id := g.node.LeaderWithID()
	return string(id)
}

// Apply는 과반수에 기록되고 이 노드의 실행기에서 적용된 후에 응답한다.
// 지도자를 잃은 시점의 요청은 결과가 불확실할 수 있어 여기서 재시도하지 않는다.
func (g *group) execute(args []string) (resp.Value, error) {
	if g.node.State() != raft.Leader {
		return resp.Value{}, errNotPrimary
	}
	future := g.node.Apply(encodeCommand(args, time.Now().UnixMilli()), time.Second)
	if err := future.Error(); err != nil {
		return resp.Value{}, err
	}
	return future.Response().(resp.Value), nil
}
