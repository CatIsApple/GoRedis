package persist

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"

	"github.com/CatIsApple/GoRedis/internal/resp"
	"github.com/CatIsApple/GoRedis/internal/store"
)

// AOF는 단일 실행기가 소유하는 Append Only File이다. 한 명령은 RESP 배열
// 하나로 기록하므로 키와 값에 개행이나 NUL 바이트가 있어도 경계를 복원할 수 있다.
type AOF struct {
	syncPolicy
	file   *os.File
	writer *resp.Writer
	failed error
}

// Open은 빈 키 공간에 AOF를 처음부터 재생한 다음, 같은 파일을 쓰기용으로
// 넘긴다. 오류를 반환하면 호출자는 부분적으로 복구된 키 공간을 버린다.
func Open(path string, db *store.Store) (*AOF, error) {
	return OpenWithFsync(path, db, "everysec")
}

func OpenWithFsync(path string, db *store.Store, policy string) (*AOF, error) {
	if err := validateFsync(policy); err != nil {
		return nil, err
	}
	f, err := openLockedFile(path)
	if err != nil {
		return nil, err
	}
	if err := replay(f, db, true); err != nil {
		f.Close()
		return nil, fmt.Errorf("replay AOF %s: %w", path, err)
	}
	// 이전 만료 시각을 뒤의 명령이 연장했을 수 있다. 전체 재생이 끝난 시점의
	// 최종 기한을 기준으로, 서버가 꺼져 있는 동안 만료된 키까지 정리한다.
	db.PurgeExpired(math.MaxInt)
	a := &AOF{file: f, writer: resp.NewWriter(f)}
	a.startFsync(policy)
	return a, nil
}

type tailMode uint8

const (
	strictTail     tailMode = iota // 이전 세대는 모든 명령이 완전해야 한다.
	strictBase                     // 이전 단일 AOF가 manifest의 base이면 손상을 허용하지 않는다.
	repairLastTail                 // 다시 쓸 활성 파일은 마지막 정상 경계에서 자른다.
	importLegacy                   // 이전 버전 원본은 그대로 두고 정상 명령까지만 가져온다.
)

func replay(f *os.File, db *store.Store, repairTail bool) error {
	mode := strictTail
	if repairTail {
		mode = repairLastTail
	}
	return replayFile(f, db, mode)
}

func replayBase(f *os.File, db *store.Store) error {
	return replayFile(f, db, strictBase)
}

func replayLegacy(f *os.File, db *store.Store) error {
	return replayFile(f, db, importLegacy)
}

func replayFile(f *os.File, db *store.Store, mode tailMode) error {
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return errors.New("AOF must be a regular file")
	}
	input := &io.LimitedReader{R: io.NewSectionReader(f, 0, info.Size()), N: info.Size()}
	buffer := bufio.NewReader(input)
	if mode == importLegacy || mode == strictBase {
		// Redis의 이전 단일 AOF는 같은 파일 앞에 RDB, 뒤에 RESP를 이어 쓸 수 있다.
		// RDB의 체크섬까지 읽은 이 버퍼를 그대로 써야 미리 읽힌 첫 명령이 보존된다.
		magic, err := buffer.Peek(5)
		if err != nil && err != io.EOF {
			return err
		}
		if string(magic) == "REDIS" {
			if err := readRDB(buffer, db); err != nil {
				return fmt.Errorf("legacy RDB preamble: %w", err)
			}
		}
	}
	// NewReader는 이미 버퍼링된 입력을 그대로 사용한다. RESP와 Redis의 #TS 주석이
	// 같은 버퍼를 읽어야 다음 레코드의 파일 위치를 정확히 계산할 수 있다.
	reader := resp.NewReader(buffer)
	var queued [][]resp.Value
	var multiStart int64 = -1
	for {
		start := info.Size() - input.N - int64(buffer.Buffered())
		args, err := readAOFCommand(buffer, reader)
		if err == io.EOF && multiStart == -1 {
			return nil
		}
		if err == io.EOF || errors.Is(err, io.ErrUnexpectedEOF) {
			if multiStart >= 0 {
				// EXEC가 파일에 도달하기 전에 종료됐으므로 MULTI부터 통째로 제거한다.
				start = multiStart
			}
			switch mode {
			case importLegacy:
				return nil
			case strictTail, strictBase:
				return fmt.Errorf("incomplete AOF at byte %d: %w", start, io.ErrUnexpectedEOF)
			}
			if err := f.Truncate(start); err != nil {
				return err
			}
			return f.Sync()
		}
		if err != nil {
			return fmt.Errorf("byte %d: %w", start, err)
		}
		if args == nil { // Redis의 선택적 AOF 타임스탬프 주석
			continue
		}
		switch strings.ToUpper(string(args[0].Bytes)) {
		case "MULTI":
			if len(args) != 1 || multiStart >= 0 {
				return fmt.Errorf("unexpected MULTI at byte %d", start)
			}
			multiStart = start
		case "EXEC":
			if len(args) != 1 || multiStart < 0 {
				return fmt.Errorf("unexpected EXEC at byte %d", start)
			}
			for _, command := range queued {
				if err := apply(db, command); err != nil {
					return fmt.Errorf("transaction at byte %d: %w", multiStart, err)
				}
			}
			queued, multiStart = nil, -1
		default:
			if multiStart >= 0 {
				queued = append(queued, args)
			} else if err := apply(db, args); err != nil {
				return fmt.Errorf("byte %d: %w", start, err)
			}
		}
	}
}

func readAOFCommand(buffer *bufio.Reader, reader *resp.Reader) ([]resp.Value, error) {
	prefix, err := buffer.Peek(1)
	if err != nil {
		return nil, err
	}
	if prefix[0] == '#' {
		_, err = buffer.ReadSlice('\n')
		if err == io.EOF {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}
	if prefix[0] != byte(resp.Array) {
		return nil, errors.New("expected an AOF command array")
	}
	record, err := reader.ReadValue()
	if err != nil {
		return nil, err
	}
	args := record.Elements
	if record.Kind != resp.Array || len(args) == 0 {
		return nil, errors.New("expected a non-empty AOF command")
	}
	for _, arg := range args {
		if arg.Kind != resp.BulkString || arg.Bytes == nil {
			return nil, errors.New("expected a non-null bulk string in AOF command")
		}
	}
	return args, nil
}

func apply(db *store.Store, args []resp.Value) error {
	switch command := strings.ToUpper(string(args[0].Bytes)); command {
	case "SELECT":
		// Redis는 증분 파일의 첫 쓰기 앞에 SELECT를 붙인다. 현재 키 공간은 DB 0이다.
		if len(args) != 2 || string(args[1].Bytes) != "0" {
			return errors.New("only SELECT 0 is supported in AOF")
		}
	case "SET":
		return replaySet(db, args)
	case "INCR":
		// 실제 Redis는 증가한 결과 대신 INCR 자체를 기록한다. 기준 스냅샷의
		// 숫자와 기한을 읽을 때는 재시작한 현재 시각을 적용하지 않는다.
		if len(args) != 2 {
			return errors.New("expected INCR key in AOF")
		}
		key := string(args[1].Bytes)
		value, found, err := db.StringAt(key, 0)
		if err != nil {
			return err
		}
		var n int64
		if found {
			n, err = strconv.ParseInt(value, 10, 64)
			if err != nil || n == math.MaxInt64 || strconv.FormatInt(n, 10) != value {
				return errors.New("invalid integer or overflow for INCR in AOF")
			}
		}
		deadline := db.ExpiresAt(key)
		db.Set(key, strconv.FormatInt(n+1, 10))
		if deadline > 0 {
			db.SetExpiryAt(key, deadline)
		}
	case "MSET":
		if len(args) < 3 || len(args)%2 == 0 {
			return errors.New("expected key/value pairs in AOF MSET")
		}
		// 원래 순서대로 덮어써서 같은 키의 마지막 값을 남기고 기존 TTL을 지운다.
		for i := 1; i < len(args); i += 2 {
			db.Set(string(args[i].Bytes), string(args[i+1].Bytes))
		}
	case "HSET", "HMSET":
		// Redis가 RDB 프리앰블 없이 재작성한 base AOF는 해시를 HMSET으로 저장한다.
		if len(args) < 4 || len(args)%2 != 0 {
			return errors.New("expected key and field/value pairs in AOF HSET")
		}
		key := string(args[1].Bytes)
		// 과거 명령을 재생하는 동안 현재 시각을 쓰면, 뒤의 EXPIRE가
		// 연장한 해시를 일찍 지울 수 있다. 유효한 만료 시각은 모두 0보다 크다.
		if kind := db.TypeAt(key, 0); kind != "none" && kind != "hash" {
			return store.ErrWrongType
		}
		fields := make([]store.Pair, len(args)/2-1)
		for i := range fields {
			fields[i] = store.Pair{Key: string(args[2+2*i].Bytes), Value: string(args[3+2*i].Bytes)}
		}
		db.HSet(key, fields)
	case "PEXPIREAT":
		if len(args) != 3 {
			return errors.New("expected PEXPIREAT key timestamp in AOF")
		}
		deadline, err := strconv.ParseInt(string(args[2].Bytes), 10, 64)
		if err != nil {
			return errors.New("invalid absolute expiration time")
		}
		db.SetExpiryAt(string(args[1].Bytes), deadline)
	case "DEL", "UNLINK":
		if len(args) < 2 {
			return errors.New("expected at least one key in AOF DEL")
		}
		for _, key := range args[1:] {
			db.Delete(string(key.Bytes))
		}
	default:
		return fmt.Errorf("unsupported AOF command %q", command)
	}
	return nil
}

// Redis는 SET EX를 절대 시각인 PXAT로 바꿔 기록한다. 만료 기한 없는 SET은
// NX 같은 원래 옵션도 그대로 기록하므로 당시 키 상태를 기준으로 처리한다.
func replaySet(db *store.Store, args []resp.Value) error {
	if len(args) < 3 {
		return errors.New("expected SET key value in AOF")
	}
	key := string(args[1].Bytes)
	var nx, xx, keepTTL bool
	var deadline int64
	for i := 3; i < len(args); i++ {
		// Redis는 옵션 문자열의 첫 NUL까지 비교하고, AOF에는 원래 바이트를 남길 수 있다.
		option, _, _ := strings.Cut(string(args[i].Bytes), "\x00")
		switch strings.ToUpper(option) {
		case "NX":
			nx = true
		case "XX":
			xx = true
		case "KEEPTTL":
			keepTTL = true
		case "PXAT":
			i++
			if i >= len(args) {
				return errors.New("missing absolute expiration time in SET")
			}
			n, err := strconv.ParseInt(string(args[i].Bytes), 10, 64)
			if err != nil || n <= 0 {
				return errors.New("invalid absolute expiration time in SET")
			}
			deadline = n
		default:
			return errors.New("unsupported SET option in AOF")
		}
	}
	if (nx && xx) || (keepTTL && deadline > 0) {
		return errors.New("incompatible SET options in AOF")
	}
	if (nx && db.ExistsAt(key, 0)) || (xx && !db.ExistsAt(key, 0)) {
		return nil
	}
	if keepTTL {
		deadline = db.ExpiresAt(key)
	}
	db.Set(key, string(args[2].Bytes))
	if deadline > 0 {
		db.SetExpiryAt(key, deadline)
	}
	return nil
}

func (a *AOF) AppendSet(key, value string) error {
	return a.append([]string{"SET", key, value})
}

// AppendMSet은 모든 쌍을 한 레코드로 기록한다. 쓰기가 중간에 끊기면
// 재생 시 이 레코드 전체를 제거해 앞쪽 키만 복구되는 일을 막는다.
func (a *AOF) AppendMSet(pairs []store.Pair) error {
	if len(pairs) == 0 {
		return errors.New("expected at least one key/value pair in AOF MSET")
	}
	return a.append(pairCommand(pairs, "MSET"))
}

// AppendHSet은 모든 필드를 HSET 한 건에 담는다. 새 해시는 같은 이름의 키가
// 자연 만료된 뒤 생성됐을 수 있으므로 DEL을 먼저 기록해 옛 필드와 TTL을 지운다.
// 두 레코드 사이에서 서버가 종료돼 DEL만 남아도 생성 직전에는 키가 없었다.
func (a *AOF) AppendHSet(key string, fields []store.Pair, create bool) error {
	if len(fields) == 0 {
		return errors.New("expected at least one field/value pair in AOF HSET")
	}
	hset := pairCommand(fields, "HSET", key)
	if create {
		return a.append([]string{"DEL", key}, hset)
	}
	return a.append(hset)
}

func pairCommand(pairs []store.Pair, prefix ...string) []string {
	args := make([]string, 0, len(prefix)+2*len(pairs))
	args = append(args, prefix...)
	for _, pair := range pairs {
		args = append(args, pair.Key, pair.Value)
	}
	return args
}

// AppendSetExpireAt은 SET EX의 값과 기한을 한 레코드로 기록한다. 중간에
// 끊긴 레코드는 통째로 제거되므로, 값만 복구되어 만료 시각을 잃지 않는다.
// NX는 실행기가 이미 확인한 조건이라 재시작할 때 다시 평가할 필요가 없다.
func (a *AOF) AppendSetExpireAt(key, value string, deadlineMS int64) error {
	if deadlineMS <= 0 {
		return errors.New("invalid absolute expiration time in SET")
	}
	return a.append([]string{"SET", key, value, "PXAT", strconv.FormatInt(deadlineMS, 10)})
}

// AppendDel은 같은 DEL로 삭제된 모든 키를 한 레코드에 보관한다.
func (a *AOF) AppendDel(keys []string) error {
	if len(keys) == 0 {
		return errors.New("expected at least one key in AOF DEL")
	}
	return a.append(append([]string{"DEL"}, keys...))
}

// AppendExpireAt은 EXPIRE가 결정한 Unix 밀리초 시각을 기록한다. 예를 들어
// 10초 만료 후 서버가 4초 동안 중단되면 재시작 시 남은 시간은 약 6초다.
func (a *AOF) AppendExpireAt(key string, deadlineMS int64) error {
	return a.append([]string{"PEXPIREAT", key, strconv.FormatInt(deadlineMS, 10)})
}

func (a *AOF) append(commands ...[]string) error {
	if a.failed != nil {
		return a.failed
	}
	// 이전 비동기 fsync가 실패하면 디스크 동기화에 성공할 때까지 새 쓰기를 보류한다.
	if err := a.syncError(); err != nil {
		return err
	}
	start, err := a.file.Seek(0, io.SeekEnd)
	if err != nil {
		a.failed = err
		return err
	}
	for _, args := range commands {
		elements := make([]resp.Value, len(args))
		for i, arg := range args {
			elements[i] = resp.Value{Kind: resp.BulkString, Bytes: []byte(arg)}
		}
		err = a.writer.WriteValue(resp.Value{Kind: resp.Array, Elements: elements})
		if err != nil {
			break
		}
	}
	// 명령의 모든 레코드를 파일에 보낸 뒤 설정된 appendfsync 정책을 적용한다.
	if err == nil {
		err = a.writer.Flush()
	}
	if err == nil {
		err = a.syncWrites()
	}
	if err != nil {
		// 일부 바이트만 기록됐을 수 있다. 이번 쓰기의 첫 레코드 경계로 되돌리고 이후
		// 쓰기를 거절해야, 손상된 접미부 뒤에 정상 명령이 이어지지 않는다.
		a.failed = errors.Join(err, a.file.Truncate(start), a.file.Sync())
		return a.failed
	}
	return nil
}

func (a *AOF) Close() error {
	a.stopFsync()
	return errors.Join(a.syncFile(a.file), a.file.Close())
}
