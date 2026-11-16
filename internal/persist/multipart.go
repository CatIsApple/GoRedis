package persist

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/CatIsApple/GoRedis/internal/resp"
	"github.com/CatIsApple/GoRedis/internal/store"
)

const initMarker = ".goredis-initializing"

// file과 writer는 명령 실행기만 교체한다. mu는 백그라운드 RDB 작성기와 공유하는
// manifest 및 크기 기준을 보호한다. 진행 중에는 done이 닫힐 때까지 다음 재작성을 막는다.
type generations struct {
	dir, prefix, manifestPath string
	lock                      *os.File
	mu                        sync.Mutex
	active                    manifest
	done                      chan struct{}
	fixedSize, rewriteSize    int64
	retryAfter                time.Time
	currentSize               int64 // 활성 INCR에 실제로 기록한 크기. 실행기만 갱신한다.
}

func Open(path string, db *store.Store) (*AOF, error) {
	return OpenWithFsync(path, db, "everysec")
}

// OpenWithFsync는 manifest가 가리키는 base부터 읽고 INCR을 순번대로 적용한다.
// 중간의 만료 기한이 뒤에서 연장될 수 있어 현재 시각으로 정리하는 작업은 항상 마지막이다.
func OpenWithFsync(path string, db *store.Store, policy string) (*AOF, error) {
	if err := validateFsync(policy); err != nil {
		return nil, err
	}
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	prefix := filepath.Base(path)
	dir := path + ".d"
	if prefix == "appendonly.aof" {
		dir = filepath.Join(filepath.Dir(path), "appendonlydir")
	}
	if err := os.Mkdir(dir, 0700); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return nil, err
		}
	}
	if err := syncDir(filepath.Dir(dir)); err != nil {
		return nil, err
	}
	lock, err := openLockedFile(filepath.Join(dir, ".goredis-lock"))
	if err != nil {
		return nil, err
	}
	a := &AOF{generations: generations{
		dir: dir, prefix: prefix, manifestPath: filepath.Join(dir, prefix+".manifest"), lock: lock,
	}}
	if err := a.openFiles(path, db); err != nil {
		if a.file != nil {
			a.file.Close()
		}
		lock.Close()
		return nil, fmt.Errorf("open persistence %s: %w", path, err)
	}
	db.PurgeExpired(math.MaxInt)
	a.startFsync(policy)
	return a, nil
}

func (a *AOF) openFiles(legacyPath string, db *store.Store) error {
	m, err := readManifest(a.manifestPath)
	if errors.Is(err, os.ErrNotExist) {
		return a.initialize(legacyPath, db)
	}
	if err != nil {
		return err
	}
	var size int64
	if m.base.name != "" {
		base := filepath.Join(a.dir, m.base.name)
		size, err = loadBase(base, db)
		if err != nil {
			return fmt.Errorf("load base %s: %w", base, err)
		}
	}
	a.rewriteSize = size
	a.fixedSize = size
	for i, p := range m.tails {
		last := i == len(m.tails)-1
		flags := os.O_RDONLY
		if last {
			flags = os.O_RDWR
		}
		f, err := os.OpenFile(filepath.Join(a.dir, p.name), flags, 0)
		if err != nil {
			return err
		}
		if err := replay(f, db, last); err != nil {
			f.Close()
			return fmt.Errorf("replay %s: %w", p.name, err)
		}
		// 복구 중 마지막 불완전한 명령이 잘렸을 수 있으므로 복구 후 크기를 쓴다.
		size, err := f.Seek(0, io.SeekEnd)
		if err != nil {
			f.Close()
			return err
		}
		if last {
			a.useIncrement(f, size)
		} else {
			a.fixedSize += size
			if err := f.Close(); err != nil {
				return err
			}
		}
	}
	a.active = m
	if a.file == nil {
		if err := a.rotateIncrement(); err != nil {
			return err
		}
	}
	return a.finishOpen()
}

func loadBase(path string, db *store.Store) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		return 0, err
	}
	if !stat.Mode().IsRegular() {
		return 0, errors.New("base must be a regular file")
	}
	if strings.HasSuffix(path, ".rdb") {
		reader := bufio.NewReader(f)
		if err := readRDB(reader, db); err != nil {
			return 0, err
		}
		return stat.Size(), endOfRDB(reader)
	}
	// Redis가 단일 AOF에서 업그레이드한 base는 한 파일에 RDB와 RESP를
	// 이어 쓸 수 있다. aof-use-rdb-preamble no로 쓴 RESP 전용 base도 읽는다.
	return stat.Size(), replayBase(f, db)
}

func endOfRDB(r *bufio.Reader) error {
	_, err := r.Peek(1)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err == nil {
		return errors.New("unexpected bytes after RDB checksum")
	}
	return err
}

// 첫 manifest가 생기기 전에는 클라이언트에게 쓰기를 받지 않는다. 초기화 표시가
// 있어야 중간 종료 후 재시도할 수 있다. 이미 쓴 파일 옆에서 manifest만 사라진
// 경우는 빈 데이터베이스로 잘못 기동하지 않도록 오류로 돌린다.
func (a *AOF) initialize(legacyPath string, db *store.Store) error {
	marker := filepath.Join(a.dir, initMarker)
	if _, err := os.Stat(marker); errors.Is(err, os.ErrNotExist) {
		files, err := os.ReadDir(a.dir)
		if err != nil {
			return err
		}
		for _, file := range files {
			if file.Name() != ".goredis-lock" {
				return errors.New("missing manifest in nonempty AOF directory")
			}
		}
		f, err := os.OpenFile(marker, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
		if err := syncDir(a.dir); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	// 단일 AOF를 쓰던 버전이 실행 중이면 같은 파일을 동시에 읽거나 복구하지 않는다.
	if _, err := os.Stat(legacyPath); err == nil {
		f, err := openLockedFile(legacyPath)
		if err != nil {
			return err
		}
		defer f.Close()
		if err := replayLegacy(f, db); err != nil {
			return fmt.Errorf("replay legacy AOF: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	base, size, err := a.writeBase(db.Entries(), 0)
	if err != nil {
		return err
	}
	a.active.base = base
	a.fixedSize, a.rewriteSize = size, size
	if err := a.rotateIncrement(); err != nil {
		return err
	}
	return a.finishOpen()
}

// manifest에 쓰인 파일만 복구한다. 이전 실행이 재작성 도중 종료해 남은
// 임시 파일과 구세대는 현재 manifest가 디스크에 안전하게 기록된 뒤 지운다.
func (a *AOF) finishOpen() error {
	err := os.Remove(filepath.Join(a.dir, initMarker))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := syncDir(a.dir); err != nil {
		return err
	}
	a.cleanObsolete(a.active, manifest{})
	return nil
}

func (a *AOF) writeBase(entries []store.Entry, lastSeq int64) (part, int64, error) {
	tmp, err := os.CreateTemp(a.dir, ".goredis-rdb-*")
	if err != nil {
		return part{}, 0, err
	}
	defer os.Remove(tmp.Name())
	w := bufio.NewWriter(tmp)
	if err = WriteSnapshot(w, entries); err == nil {
		err = w.Flush()
	}
	if err == nil {
		err = tmp.Sync()
	}
	size, statErr := tmp.Seek(0, io.SeekCurrent)
	if err = errors.Join(err, statErr, tmp.Close()); err != nil {
		return part{}, 0, err
	}
	// 완성된 임시 파일을 새 세대로 옮긴다. 경로 예약은 이미 있는 파일을
	// 덮어쓰지 않게 하며, 다음 manifest 교체까지 이전 base가 계속 유효하다.
	p, reserved, err := newPart(a.dir, a.prefix, "base.rdb", lastSeq)
	if err != nil {
		return part{}, 0, err
	}
	if err = reserved.Close(); err == nil {
		err = replaceManifest(tmp.Name(), reserved.Name())
	}
	if err != nil {
		os.Remove(reserved.Name())
		return part{}, 0, err
	}
	return p, size, nil
}

type incrementWriter struct {
	f    *os.File
	size *int64
}

func (w incrementWriter) Write(p []byte) (int, error) {
	n, err := w.f.Write(p)
	*w.size += int64(n)
	return n, err
}

func (a *AOF) useIncrement(f *os.File, size int64) {
	a.file, a.currentSize = f, size
	a.writer = resp.NewWriter(incrementWriter{f, &a.currentSize})
}
