package persist

import (
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"

	"github.com/CatIsApple/GoRedis/internal/resp"
	"github.com/CatIsApple/GoRedis/internal/store"
)

// AOF는 단일 실행기가 소유하는 Append Only File이다. 한 명령은 RESP 배열
// 하나로 기록하므로 키와 값에 개행이나 NUL 바이트가 있어도 경계를 복원할 수 있다.
type AOF struct {
	file   *os.File
	writer *resp.Writer
	failed error
}

// Open은 빈 키 공간에 AOF를 처음부터 재생한 다음, 같은 파일을 쓰기용으로
// 넘긴다. 오류를 반환하면 호출자는 부분적으로 복구된 키 공간을 버린다.
func Open(path string, db *store.Store) (*AOF, error) {
	f, err := openLockedFile(path)
	if err != nil {
		return nil, err
	}
	if err := replay(f, db); err != nil {
		f.Close()
		return nil, fmt.Errorf("replay AOF %s: %w", path, err)
	}
	// 이전 만료 시각을 뒤의 명령이 연장했을 수 있다. 전체 재생이 끝난 시점의
	// 최종 기한을 기준으로, 서버가 꺼져 있는 동안 만료된 키까지 정리한다.
	db.PurgeExpired(math.MaxInt)
	return &AOF{file: f, writer: resp.NewWriter(f)}, nil
}

func replay(f *os.File, db *store.Store) error {
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return errors.New("AOF must be a regular file")
	}
	input := &io.LimitedReader{R: f, N: info.Size()}
	reader := resp.NewReader(input)
	for {
		// bufio는 다음 명령의 바이트도 미리 읽는다. 파일에서 읽어온 양에서
		// 아직 소비하지 않은 양을 빼야 현재 명령의 정확한 시작 위치가 나온다.
		start := info.Size() - input.N - int64(reader.Buffered())
		record, err := reader.ReadValue()
		switch {
		case err == io.EOF:
			return nil
		case errors.Is(err, io.ErrUnexpectedEOF):
			// 마지막 레코드를 쓰는 도중 종료됐다. 정상 접두부를 보존하고
			// 불완전한 바이트만 제거한 뒤 다음 쓰기를 같은 경계에서 시작한다.
			if err := f.Truncate(start); err != nil {
				return err
			}
			return f.Sync()
		case err != nil:
			return fmt.Errorf("byte %d: %w", start, err)
		}
		if err := apply(db, record); err != nil {
			return fmt.Errorf("byte %d: %w", start, err)
		}
	}
}

func apply(db *store.Store, record resp.Value) error {
	args := record.Elements
	if record.Kind != resp.Array || len(args) != 3 {
		return errors.New("expected a three-element AOF command")
	}
	for _, arg := range args {
		if arg.Kind != resp.BulkString || arg.Bytes == nil {
			return errors.New("expected a non-null bulk string in AOF command")
		}
	}
	key := string(args[1].Bytes)
	switch string(args[0].Bytes) {
	case "SET":
		db.Set(key, string(args[2].Bytes))
	case "PEXPIREAT":
		deadline, err := strconv.ParseInt(string(args[2].Bytes), 10, 64)
		if err != nil {
			return errors.New("invalid absolute expiration time")
		}
		db.SetExpiryAt(key, deadline)
	default:
		return errors.New("unsupported AOF command")
	}
	return nil
}

func (a *AOF) AppendSet(key, value string) error {
	return a.append("SET", key, value)
}

// AppendExpireAt은 EXPIRE가 결정한 Unix 밀리초 시각을 기록한다. 예를 들어
// 10초 만료 후 서버가 4초 동안 중단되면 재시작 시 남은 시간은 약 6초다.
func (a *AOF) AppendExpireAt(key string, deadlineMS int64) error {
	return a.append("PEXPIREAT", key, strconv.FormatInt(deadlineMS, 10))
}

func (a *AOF) append(command, key, value string) error {
	if a.failed != nil {
		return a.failed
	}
	start, err := a.file.Seek(0, io.SeekEnd)
	if err != nil {
		a.failed = err
		return err
	}
	record := resp.Value{Kind: resp.Array, Elements: []resp.Value{
		{Kind: resp.BulkString, Bytes: []byte(command)},
		{Kind: resp.BulkString, Bytes: []byte(key)},
		{Kind: resp.BulkString, Bytes: []byte(value)},
	}}
	// Flush는 Go의 버퍼를 파일로 보내고 Sync는 운영체제에 영속화를 요청한다.
	// 둘 다 성공해야 실행기가 메모리를 변경하고 클라이언트에 성공을 응답한다.
	if err = a.writer.WriteValue(record); err == nil {
		err = a.writer.Flush()
	}
	if err == nil {
		err = a.file.Sync()
	}
	if err != nil {
		// 일부 바이트만 기록됐을 수 있다. 직전 레코드 경계로 되돌리고 이후
		// 쓰기를 거절해야, 손상된 접미부 뒤에 정상 명령이 이어지지 않는다.
		a.failed = errors.Join(err, a.file.Truncate(start), a.file.Sync())
		return a.failed
	}
	return nil
}

// Close는 파일과 파일 잠금을 함께 해제한다. 개별 명령은 이미 Sync를 마쳤다.
func (a *AOF) Close() error {
	return a.file.Close()
}
