package persist

import (
	"errors"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/CatIsApple/GoRedis/internal/store"
)

var ErrRewriteInProgress = errors.New("background append only file rewriting already in progress")

// rotateIncrement은 새 INCR을 만든 뒤, 아직은 이전 base와 모든 INCR이 필요한
// manifest를 먼저 공개한다. 이 시점에 죽어도 이전 base부터 새 쓰기까지 모두 재생된다.
// 실행기에서만 부르므로 manifest와 쓰기 파일의 교체 사이에 다른 명령이 끼어들지 않는다.
func (a *AOF) rotateIncrement() error {
	if a.file != nil {
		if err := a.syncFile(a.file); err != nil {
			return err
		}
	}
	m := a.active
	var seq int64
	if n := len(m.tails); n > 0 {
		seq = m.tails[n-1].seq
	}
	p, f, err := newPart(a.dir, a.prefix, "incr.aof", seq)
	if err != nil {
		return err
	}
	m.tails = append(m.tails, p)
	// manifest보다 먼저 새 base/INCR의 디렉터리 항목과 파일 내용을 동기화한다.
	var published bool
	if err = f.Sync(); err == nil {
		err = syncDir(a.dir)
	}
	if err == nil {
		published, err = installManifest(a.manifestPath, m)
	}
	if !published {
		f.Close()
		os.Remove(f.Name())
		return err
	}
	old := a.file
	a.active = m
	a.fixedSize += a.currentSize
	a.useIncrement(f, 0)
	if old != nil {
		err = errors.Join(err, old.Close())
	}
	if err != nil {
		// rename은 보였지만 디렉터리 동기화에 실패했다. 어느 manifest가 살아남을지
		// 확정할 수 없으므로 새 INCR 단독으로 클라이언트 쓰기를 승인하지 않는다.
		a.failed = err
	}
	return err
}

// Rewrite는 단일 실행기 안에서 새 INCR로 전환하고 같은 시점의 키 공간을 캡처한다.
// 이후 디스크 인코딩과 manifest 교체에는 store 접근이 없어 요청 처리를 재개할 수 있다.
func (a *AOF) Rewrite(db *store.Store) error {
	a.mu.Lock()
	if a.done != nil {
		a.mu.Unlock()
		return ErrRewriteInProgress
	}
	if a.failed != nil {
		a.mu.Unlock()
		return a.failed
	}
	if err := a.rotateIncrement(); err != nil {
		a.retryAfter = time.Now().Add(time.Minute)
		a.mu.Unlock()
		return err
	}
	entries := db.Entries()
	current := a.active
	a.done = make(chan struct{})
	a.mu.Unlock()

	go func() {
		err := a.writeGeneration(entries, current)
		a.mu.Lock()
		if err != nil {
			a.retryAfter = time.Now().Add(time.Minute)
			log.Printf("AOF background rewrite: %v", err)
		}
		close(a.done)
		a.done = nil
		a.mu.Unlock()
	}()
	return nil
}

func (a *AOF) writeGeneration(entries []store.Entry, previous manifest) error {
	base, size, err := a.writeBase(entries, previous.base.seq)
	if err != nil {
		return err
	}
	tail := previous.tails[len(previous.tails)-1]
	next := manifest{base: base, tails: []part{tail}}
	if err := syncDir(a.dir); err != nil {
		os.Remove(filepath.Join(a.dir, base.name))
		return err
	}
	published, err := installManifest(a.manifestPath, next)
	if !published {
		os.Remove(filepath.Join(a.dir, base.name))
		return err
	}
	// background 쓰기 중 추가된 INCR까지 현재 크기 기준에 포함한다.
	// next와 previous 모두 동일한 마지막 INCR을 재생하므로 이후 쓰기는 안전하다.
	total := size
	if info, statErr := os.Stat(filepath.Join(a.dir, tail.name)); statErr == nil {
		total += info.Size()
	}
	a.mu.Lock()
	a.active, a.fixedSize, a.rewriteSize = next, size, total
	a.mu.Unlock()
	if err == nil {
		a.cleanObsolete(next, previous)
	}
	return err
}

// ShouldRewrite는 Redis 기본 기준(64 MiB 초과, 마지막 기준 크기에서 100% 이상 증가)을
// 적용한다. 각 쓰기의 바이트 수를 이미 세어 두므로 타이머에서 파일을 다시 열지 않는다.
func (a *AOF) ShouldRewrite() bool {
	if !a.mu.TryLock() {
		return false
	}
	defer a.mu.Unlock()
	if a.done != nil || a.failed != nil || time.Now().Before(a.retryAfter) {
		return false
	}
	size := a.fixedSize + a.currentSize
	return size > 64<<20 && size-a.rewriteSize >= a.rewriteSize
}

func (a *AOF) cleanObsolete(m, previous manifest) {
	files, err := os.ReadDir(a.dir)
	if err != nil {
		log.Printf("AOF cleanup: %v", err)
		return
	}
	keep := map[string]bool{m.base.name: true}
	for _, tail := range m.tails {
		keep[tail.name] = true
	}
	// Redis가 이전 단일 AOF를 base로 가져온 경우에는 파일명에 순번이 없다.
	// 방금 교체한 manifest에서 사용한 파일도 새 manifest에 없으면 지울 수 있다.
	retired := map[string]bool{previous.base.name: true}
	for _, tail := range previous.tails {
		retired[tail.name] = true
	}
	for _, file := range files {
		name := file.Name()
		if keep[name] || !file.Type().IsRegular() {
			continue
		}
		// 이 인스턴스가 생성한 이름만 정리한다. 임의의 사용자 파일은 건드리지 않는다.
		temp := false
		for _, prefix := range []string{".goredis-rdb-", ".goredis-manifest-"} {
			if num, ok := strings.CutPrefix(name, prefix); ok {
				_, err := strconv.ParseUint(num, 10, 32)
				temp = err == nil
			}
		}
		s, prefix := strings.CutPrefix(name, a.prefix+".")
		seq, suffix, dot := strings.Cut(s, ".")
		n, err := strconv.ParseInt(seq, 10, 64)
		managed := prefix && dot && err == nil && n > 0 && strconv.FormatInt(n, 10) == seq &&
			(suffix == "base.rdb" || suffix == "incr.aof" || suffix == "base.aof")
		if temp || managed || retired[name] {
			if err := os.Remove(filepath.Join(a.dir, name)); err != nil {
				log.Printf("AOF cleanup %s: %v", name, err)
			}
		}
	}
}

// Close는 실행기가 멈춘 뒤 호출한다. 백그라운드 작성기가 manifest를 교체하고
// 마지막 INCR이 동기화된 다음에만 다른 서버가 같은 디렉터리를 열 수 있다.
func (a *AOF) Close() error {
	a.mu.Lock()
	done := a.done
	a.mu.Unlock()
	if done != nil {
		<-done
	}
	a.stopFsync()
	return errors.Join(a.syncFile(a.file), a.file.Close(), a.lock.Close())
}
