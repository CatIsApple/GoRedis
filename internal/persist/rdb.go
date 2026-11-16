package persist

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc64"
	"io"
	"math"
	"strconv"

	"github.com/CatIsApple/GoRedis/internal/store"
)

// Redis 7.2와 8 모두 읽을 수 있는 RDB v11을 쓴다. 길이는 가변 길이 정수,
// 만료 시각과 파일 끝의 CRC64는 little-endian이다.
const (
	rdbHeader = "REDIS0011"

	rdbString = 0
	rdbHash   = 4

	rdbIdle      = 248
	rdbFrequency = 249
	rdbAux       = 250
	rdbResizeDB  = 251
	rdbExpireMS  = 252
	rdbExpireSec = 253
	rdbSelectDB  = 254
	rdbEOF       = 255

	rdbMaxString = 512 << 20
)

// Redis CRC64(Jones)는 초깃값 0으로 시작하고 마지막 비트 반전 없이 끝난다.
var rdbCRCTable = crc64.MakeTable(0x95ac9329ac4bc9b5)

type rdbChecksum uint64

func (c *rdbChecksum) Write(data []byte) (int, error) {
	sum := uint64(*c)
	for _, b := range data {
		sum = rdbCRCTable[byte(sum)^b] ^ (sum >> 8)
	}
	*c = rdbChecksum(sum)
	return len(data), nil
}

// WriteSnapshot은 실행기가 한 시점에 복사한 키 공간을 받아 파일에 순서대로 쓴다.
// Redis의 일반 문자열과 해시 인코딩은 압축 없이도 임의의 바이너리 값을 보존한다.
func WriteSnapshot(dst io.Writer, entries []store.Entry) error {
	var sum rdbChecksum
	w := bufio.NewWriter(io.MultiWriter(dst, &sum))
	// bufio.Writer는 첫 쓰기 오류를 보관해 마지막 Flush에서 반환한다.
	w.WriteString(rdbHeader)
	w.WriteByte(rdbSelectDB)
	w.WriteByte(0)
	for _, entry := range entries {
		if entry.Deleted || entry.ExpiresAt < 0 || (entry.Hash != nil && len(entry.Hash) == 0) {
			return errors.New("invalid entry in RDB snapshot")
		}
		if entry.ExpiresAt != 0 {
			w.WriteByte(rdbExpireMS)
			w.Write(binary.LittleEndian.AppendUint64(nil, uint64(entry.ExpiresAt)))
		}
		if entry.Hash == nil {
			w.WriteByte(rdbString)
			WriteSnapshotString(w, entry.Key)
			WriteSnapshotString(w, entry.Value)
			continue
		}
		w.WriteByte(rdbHash)
		WriteSnapshotString(w, entry.Key)
		WriteSnapshotLen(w, uint64(len(entry.Hash)))
		for _, pair := range entry.Hash {
			WriteSnapshotString(w, pair.Key)
			WriteSnapshotString(w, pair.Value)
		}
	}
	w.WriteByte(rdbEOF)
	if err := w.Flush(); err != nil {
		return err
	}
	// CRC는 헤더부터 EOF 마커까지를 포함한다. 체크섬 자체는 계산에서 뺀다.
	n, err := dst.Write(binary.LittleEndian.AppendUint64(nil, uint64(sum)))
	if n != 8 && err == nil {
		return io.ErrShortWrite
	}
	return err
}

func WriteSnapshotString(w *bufio.Writer, value string) {
	WriteSnapshotLen(w, uint64(len(value)))
	w.WriteString(value)
}

func WriteSnapshotLen(w *bufio.Writer, n uint64) {
	switch {
	case n < 1<<6:
		w.WriteByte(byte(n))
	case n < 1<<14:
		w.WriteByte(0x40 | byte(n>>8))
		w.WriteByte(byte(n))
	case n <= math.MaxUint32:
		w.WriteByte(0x80)
		w.Write(binary.BigEndian.AppendUint32(nil, uint32(n)))
	default:
		w.WriteByte(0x81)
		w.Write(binary.BigEndian.AppendUint64(nil, n))
	}
}

// ReadSnapshot은 CRC와 파일 끝을 확인한 후 완성된 키 공간의 소유권을 넘긴다.
// 뒤에 재생할 로그가 TTL을 연장할 수 있으므로 현재 시각으로 키를 제거하지 않는다.
func ReadSnapshot(src io.Reader) (*store.Store, error) {
	db := store.New()
	r := bufio.NewReader(src)
	if err := readRDB(r, db); err != nil {
		return nil, err
	}
	if err := endOfRDB(r); err != nil {
		return nil, err
	}
	return db, nil
}

// 스냅샷을 다 읽거나 오류가 나기 전까지는 호출자가 db를 공개하지 않는다.
// 특히 지금은 만료된 키라도 AOF에 기한 연장이 기록됐을 수 있으므로 일단 복원한다.
func readRDB(src io.Reader, db *store.Store) error {
	var sum rdbChecksum
	r := rdbDecoder{in: io.TeeReader(src, &sum)}
	var header [9]byte
	if _, err := io.ReadFull(r.in, header[:]); err != nil {
		return err
	}
	version, err := strconv.ParseUint(string(header[5:]), 10, 16)
	if string(header[:5]) != "REDIS" || err != nil || version < 9 || version > 15 {
		return fmt.Errorf("unsupported RDB header %q", header[:])
	}

	var deadline int64
	var expiring bool
	for r.err == nil {
		kind := r.number(1)[0]
		if r.err != nil {
			return r.err
		}
		switch kind {
		case rdbEOF:
			if expiring {
				return errors.New("missing RDB key after expiry")
			}
			var footer [8]byte
			if _, err := io.ReadFull(src, footer[:]); err != nil {
				return err
			}
			// Redis에서 rdbchecksum을 껐을 때는 0을 기록한다.
			if expected := binary.LittleEndian.Uint64(footer[:]); expected != 0 && expected != uint64(sum) {
				return errors.New("RDB checksum mismatch")
			}
			return nil
		case rdbSelectDB:
			if r.length() != 0 {
				return errors.New("only RDB database 0 is supported")
			}
		case rdbResizeDB:
			r.length() // 키와 TTL 개수는 Redis 해시 테이블 크기 산정용 힌트다.
			r.length()
		case rdbAux:
			r.bytes() // AUX는 서버 버전 등 키 공간 위에 덧붙인 메타데이터다.
			r.bytes()
		case rdbIdle:
			r.length()
		case rdbFrequency:
			r.number(1)
		case rdbExpireMS:
			deadline = int64(binary.LittleEndian.Uint64(r.number(8)))
			expiring = true
		case rdbExpireSec:
			deadline = int64(int32(binary.LittleEndian.Uint32(r.number(4)))) * 1000
			expiring = true
		case rdbString, rdbHash:
			key := string(r.bytes())
			if r.err != nil {
				return r.err
			}
			if db.ExistsAt(key, 0) {
				return errors.New("duplicate key in RDB")
			}
			r.loadValue(db, key, kind)
			if r.err == nil && expiring {
				db.SetExpiryAt(key, deadline)
			}
			expiring = false
		default:
			return fmt.Errorf("unsupported RDB type or opcode %d", kind)
		}
	}
	return r.err
}

type rdbDecoder struct {
	in  io.Reader
	buf [8]byte
	err error
}

func (r *rdbDecoder) loadValue(db *store.Store, key string, kind byte) {
	switch kind {
	case rdbString:
		value := r.bytes()
		if r.err == nil {
			db.Set(key, string(value))
		}
	case rdbHash:
		fields := r.length()
		if fields == 0 && r.err == nil {
			r.err = errors.New("empty hash in RDB")
		}
		for i := uint64(0); i < fields && r.err == nil; i++ {
			pair := store.Pair{Key: string(r.bytes()), Value: string(r.bytes())}
			if r.err == nil && db.HSet(key, []store.Pair{pair}) != 1 {
				r.err = errors.New("duplicate hash field in RDB")
			}
		}

	}
}

// 기본 숫자는 8바이트 이하이므로 같은 버퍼를 재사용한다. 한 번 읽기에 실패하면
// 첫 오류를 보존하며 더 읽지 않는다. 반환된 바이트는 다음 호출 전에 해석해야 한다.
func (r *rdbDecoder) number(n int) []byte {
	b := r.buf[:n]
	if r.err == nil {
		_, r.err = io.ReadFull(r.in, b)
	}
	return b
}

func (r *rdbDecoder) length() uint64 {
	n, encoded := r.lengthOrEncoding()
	if encoded && r.err == nil {
		r.err = errors.New("expected a plain length in RDB")
	}
	return n
}

func (r *rdbDecoder) lengthOrEncoding() (uint64, bool) {
	b := r.number(1)[0]
	switch b >> 6 {
	case 0:
		return uint64(b), false
	case 1:
		return uint64(b&0x3f)<<8 | uint64(r.number(1)[0]), false
	case 3:
		return uint64(b & 0x3f), true
	}
	switch b {
	case 0x80:
		return uint64(binary.BigEndian.Uint32(r.number(4))), false
	case 0x81:
		return binary.BigEndian.Uint64(r.number(8)), false
	}
	if r.err == nil {
		r.err = errors.New("invalid RDB length encoding")
	}
	return 0, false
}

func (r *rdbDecoder) bytes() []byte {
	n, encoded := r.lengthOrEncoding()
	if r.err != nil {
		return nil
	}
	if !encoded {
		return r.payload(n)
	}
	// Redis는 십진 문자열을 부호 있는 정수로 저장할 수 있다.
	var integer int64
	switch n {
	case 0:
		integer = int64(int8(r.number(1)[0]))
	case 1:
		integer = int64(int16(binary.LittleEndian.Uint16(r.number(2))))
	case 2:
		integer = int64(int32(binary.LittleEndian.Uint32(r.number(4))))
	default:
		r.err = fmt.Errorf("unsupported RDB string encoding %d", n)
		return nil
	}
	return strconv.AppendInt(nil, integer, 10)
}

func (r *rdbDecoder) payload(n uint64) []byte {
	if r.err != nil {
		return nil
	}
	if n > rdbMaxString {
		r.err = errors.New("RDB string exceeds size limit")
		return nil
	}
	if n <= 64<<10 {
		data := make([]byte, n)
		_, r.err = io.ReadFull(r.in, data)
		return data
	}
	// 잘린 파일이 큰 길이를 선언해도 실제로 도착한 양만큼만 메모리를 늘린다.
	data, err := io.ReadAll(io.LimitReader(r.in, int64(n)))
	if err == nil && uint64(len(data)) != n {
		err = io.ErrUnexpectedEOF
	}
	r.err = err
	return data
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
