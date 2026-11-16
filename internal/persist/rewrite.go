package persist

import (
	"errors"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

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

func (a *AOF) Close() error {
	a.stopFsync()
	return errors.Join(a.syncFile(a.file), a.file.Close(), a.lock.Close())
}
