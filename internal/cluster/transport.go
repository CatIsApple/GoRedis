package cluster

import (
	"crypto/sha256"
	"errors"
	"io"
	"net"
	"os"
	"time"

	"github.com/hashicorp/raft"
)

// 새 데이터 디렉터리로 잘못된 토폴로지를 시작해도 기존 샤드의 표를 얻거나
// 로그를 받을 수 없도록 Raft 메시지를 읽기 전에 샤드 식별자를 확인한다.
type shardStream struct {
	net.Listener
	shard    string
	endpoint raftEndpoint
	selfTag  [sha256.Size]byte
}

func openTransport(address, topologyID, firstMember string) (*raft.NetworkTransport, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	scope := topologyID + "\x00" + firstMember + "\x00"
	stream := &shardStream{
		Listener: listener,
		shard:    scope,
		endpoint: raftEndpoint(address),
		selfTag:  sha256.Sum256([]byte(scope + address)),
	}
	return raft.NewNetworkTransport(stream, 3, time.Second, os.Stderr), nil
}

// Raft가 피어에 알리는 주소는 설정의 철자를 유지한다. DNS로 접속해도
// 리스너의 해석된 IP 주소 때문에 투표자 식별자가 바뀌지 않는다.
type raftEndpoint string

func (a raftEndpoint) Network() string { return "tcp" }
func (a raftEndpoint) String() string  { return string(a) }
func (s *shardStream) Addr() net.Addr  { return s.endpoint }

func (s *shardStream) Dial(addr raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", string(addr), timeout)
	if err != nil {
		return nil, err
	}
	if err = conn.SetWriteDeadline(time.Now().Add(timeout)); err == nil {
		// 같은 IP:포트에 두 이름이 해석되어도 한 노드는 자기 주소로 보낸
		// 요청에만 응답한다. 다른 투표자의 표로 중복 집계될 수 없다.
		tag := sha256.Sum256([]byte(s.shard + string(addr)))
		_, err = conn.Write(tag[:])
	}
	if err == nil {
		err = conn.SetWriteDeadline(time.Time{})
	}
	if err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}

func (s *shardStream) Accept() (net.Conn, error) {
	conn, err := s.Listener.Accept()
	if err != nil {
		return nil, err
	}
	// Raft는 각 연결을 별도 고루틴에서 읽는다. 느린 상대의 첫 바이트를
	// 여기서 기다리면 다른 replica까지 Accept를 기다려야 한다.
	return &shardConn{Conn: conn, expected: &s.selfTag}, nil
}

type shardConn struct {
	net.Conn
	expected *[sha256.Size]byte
}

func (c *shardConn) Read(b []byte) (int, error) {
	if c.expected != nil {
		if err := c.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
			return 0, err
		}
		var got [sha256.Size]byte
		if _, err := io.ReadFull(c.Conn, got[:]); err != nil {
			return 0, err
		}
		if got != *c.expected {
			return 0, errors.New("raft connection targets a different node, shard or topology")
		}
		if err := c.SetReadDeadline(time.Time{}); err != nil {
			return 0, err
		}
		c.expected = nil
	}
	return c.Conn.Read(b)
}
