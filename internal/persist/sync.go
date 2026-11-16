package persist

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

// syncPolicy는 활성 INCR 파일을 언제 디스크에 동기화할지 결정한다.
// everysec에서 실행기는 Flush까지 끝낸 파일을 넘기고 다음 요청을 처리한다.
type syncPolicy struct {
	policy  string
	stop    chan struct{}
	stopped chan struct{}

	pendingMu sync.Mutex
	pending   *os.File // 마지막 fsync 이후 쓰인 파일
	lastErr   error
	diskMu    sync.Mutex // 이전 파일을 닫기 전에 그 파일의 백그라운드 fsync를 기다린다.
}

func validateFsync(policy string) error {
	switch policy {
	case "always", "everysec", "no":
		return nil
	default:
		return fmt.Errorf("unknown appendfsync policy %q (use always, everysec or no)", policy)
	}
}

func (a *AOF) startFsync(policy string) {
	a.policy = policy
	if policy != "everysec" {
		return
	}
	a.stop, a.stopped = make(chan struct{}), make(chan struct{})
	go func() {
		defer close(a.stopped)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-a.stop:
				return
			case <-ticker.C:
				a.syncPending()
			}
		}
	}()
}

func (p *syncPolicy) stopFsync() {
	if p.stop != nil {
		close(p.stop)
		<-p.stopped
	}
}

// syncError는 실패한 everysec 동기화를 다음 시도에서 복구할 때까지 새 쓰기를 막는다.
func (p *syncPolicy) syncError() error {
	if p.policy != "everysec" {
		return nil
	}
	p.pendingMu.Lock()
	defer p.pendingMu.Unlock()
	return p.lastErr
}

func (a *AOF) syncWrites() error {
	switch a.policy {
	case "everysec":
		a.pendingMu.Lock()
		a.pending = a.file
		a.pendingMu.Unlock()
		return nil
	case "no":
		return nil
	default:
		return a.file.Sync()
	}
}

func (p *syncPolicy) syncPending() {
	p.diskMu.Lock()
	defer p.diskMu.Unlock()
	p.pendingMu.Lock()
	file := p.pending
	p.pending = nil
	p.pendingMu.Unlock()
	if file == nil {
		return
	}

	// 느린 fsync 중에도 실행기는 같은 파일에 다음 쓰기를 기록할 수 있다.
	// 그때 설정된 pending은 그대로 남겨 다음 tick에서 다시 동기화한다.
	err := file.Sync()
	p.pendingMu.Lock()
	if err != nil {
		if p.lastErr == nil {
			log.Printf("AOF background fsync: %v", err)
		}
		p.pending = file
	}
	p.lastErr = err
	p.pendingMu.Unlock()
}

// 파일 교체와 정상 종료는 정책에 관계없이 마지막 쓰기의 동기화를 기다린다.
// 호출 시점에는 실행기가 이 파일에 새 명령을 추가하지 않는다.
func (p *syncPolicy) syncFile(file *os.File) error {
	p.diskMu.Lock()
	defer p.diskMu.Unlock()
	err := file.Sync()
	if p.policy == "everysec" {
		p.pendingMu.Lock()
		p.lastErr = err
		if err != nil {
			p.pending = file
		} else if p.pending == file {
			p.pending = nil
		}
		p.pendingMu.Unlock()
	}
	return err
}
