// Package cluster는 고정된 슬롯 배치와 샤드별 Raft 그룹을 RESP 클라이언트에 연결한다.
package cluster

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
)

// Shards의 배열 순서가 슬롯 범위를 결정한다. 각 노드는 한 샤드의 투표자이며
// Addr는 클라이언트용, Raft는 같은 샤드의 로그 복제와 선거용 TCP 주소다.
type Config struct {
	Shards []Shard `json:"shards"`
}

type Shard struct {
	Nodes []Node `json:"nodes"`
}

type Node struct {
	Addr string `json:"addr"`
	Raft string `json:"raft"`
}

// 배치를 읽으면서 현재 프로세스의 샤드와 노드도 한 번만 결정한다.
type topology struct {
	config     Config
	shard      int
	local      Node
	topologyID string
}

func readConfig(path, addr string) (*topology, error) {
	var config Config
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	decoder := json.NewDecoder(f)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return nil, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, errors.New("cluster config must contain exactly one JSON object")
	}
	if len(config.Shards) < 1 || len(config.Shards) > slotCount {
		return nil, fmt.Errorf("cluster config requires 1 to %d shards", slotCount)
	}
	used := make(map[string]bool)
	localShard := -1
	var local Node
	for i, shard := range config.Shards {
		if len(shard.Nodes) < 3 {
			return nil, fmt.Errorf("shard %d needs at least three voting nodes to tolerate a primary failure", i)
		}
		for _, node := range shard.Nodes {
			for _, endpoint := range []string{node.Addr, node.Raft} {
				if !validEndpoint(endpoint) {
					return nil, fmt.Errorf("shard %d: invalid advertised address %q", i, endpoint)
				}
				if used[endpoint] {
					return nil, fmt.Errorf("duplicate cluster address %q", endpoint)
				}
				used[endpoint] = true
			}
			if node.Addr == addr {
				localShard, local = i, node
			}
		}
	}
	if localShard < 0 {
		return nil, fmt.Errorf("listen address %q is missing from cluster config", addr)
	}
	// JSON 공백은 무시하고 슬롯·구성원 배치를 식별한다. 같은 디렉터리에
	// 다른 배치로 시작하면 과거 키가 다른 슬롯의 데이터로 해석될 수 있다.
	canonical, err := json.Marshal(config)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(canonical)
	return &topology{config: config, shard: localShard, local: local, topologyID: hex.EncodeToString(sum[:])}, nil
}

func validEndpoint(endpoint string) bool {
	host, port, err := net.SplitHostPort(endpoint)
	if err != nil || host == "" || strings.ContainsAny(host, "/\\ \t\r\n") || net.ParseIP(host).IsUnspecified() {
		return false
	}
	p, err := strconv.Atoi(port)
	return err == nil && p >= 1 && p <= 65535 && strconv.Itoa(p) == port
}
