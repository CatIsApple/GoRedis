package persist

import (
	"encoding/binary"
	"errors"
	"math"
	"math/bits"
	"strconv"

	"github.com/CatIsApple/GoRedis/internal/store"
)

var errPackedHash = errors.New("invalid RDB packed hash")

// Redis의 작은 해시는 필드와 값을 번갈아 놓는다. listpack은 각 원소 뒤에
// 자신의 길이를, 이전 형식인 ziplist는 원소 앞에 직전 원소의 길이를 적는다.
func decodePackedHash(src []byte, listpack bool) ([]store.Pair, error) {
	header := 10 // ziplist: 전체 바이트 수, 마지막 원소 위치, 원소 수
	if listpack {
		header = 6 // listpack: 전체 바이트 수, 원소 수
	}
	if len(src) <= header || uint64(binary.LittleEndian.Uint32(src)) != uint64(len(src)) || src[len(src)-1] != 0xff {
		return nil, errPackedHash
	}
	expected := binary.LittleEndian.Uint16(src[header-2:])
	data := src[header : len(src)-1]
	var pairs []store.Pair
	seen := make(map[string]struct{})
	var field string
	var previous, count int
	for len(data) != 0 {
		var value string
		var size int
		var err error
		if listpack {
			value, size, err = listpackEntry(data)
		} else {
			value, size, err = ziplistEntry(data, previous)
		}
		if err != nil {
			return nil, err
		}
		if count%2 == 0 {
			field = value
			if _, exists := seen[field]; exists {
				return nil, errPackedHash
			}
			seen[field] = struct{}{}
		} else {
			pairs = append(pairs, store.Pair{Key: field, Value: value})
		}
		count++
		previous = size
		data = data[size:]
	}
	// 65535는 정확한 원소 수를 헤더에 기록하지 않았다는 표시다.
	if count == 0 || count%2 != 0 || (expected != math.MaxUint16 && int(expected) != count) {
		return nil, errPackedHash
	}
	if !listpack && uint64(binary.LittleEndian.Uint32(src[4:])) != uint64(len(src)-1-previous) {
		return nil, errPackedHash
	}
	return pairs, nil
}

func listpackEntry(data []byte) (string, int, error) {
	tag := data[0]
	var value string
	var size int
	var err error
	switch {
	case tag < 0x80:
		value, size = strconv.Itoa(int(tag)), 1
	case tag < 0xc0:
		value, size, err = packedString(data, 1, uint32(tag&0x3f))
	case tag < 0xe0 && len(data) >= 2:
		n := int16(tag&0x1f)<<8 | int16(data[1])
		value, size = strconv.Itoa(int(n<<3>>3)), 2 // 부호 있는 13비트
	case tag >= 0xe0 && tag < 0xf0 && len(data) >= 2:
		value, size, err = packedString(data, 2, uint32(tag&0x0f)<<8|uint32(data[1]))
	case tag == 0xf0 && len(data) >= 5:
		value, size, err = packedString(data, 5, binary.LittleEndian.Uint32(data[1:]))
	case tag >= 0xf1 && tag <= 0xf4:
		width := [...]int{2, 3, 4, 8}
		value, size, err = packedInteger(data, width[tag-0xf1])
	default:
		return "", 0, errPackedHash
	}
	if err != nil {
		return "", 0, err
	}

	// 역방향 탐색용 길이는 상위 7비트 묶음부터 쓴다. 첫 바이트 이후에는
	// 최상위 비트가 1이다. Redis 7은 세 경곗값에서 앞에 0 한 바이트를 더 쓴다.
	width := (bits.Len(uint(size)) + 6) / 7
	if len(data) > size && data[size] == 0 && (size == 16383 || size == 2097151 || size == 268435455) {
		width++
	}
	if len(data)-size < width {
		return "", 0, errPackedHash
	}
	for i, b := range data[size : size+width] {
		want := byte((size >> (7 * (width - 1 - i))) & 0x7f)
		if i != 0 {
			want |= 0x80
		}
		if b != want {
			return "", 0, errPackedHash
		}
	}
	return value, size + width, nil
}

func ziplistEntry(data []byte, previous int) (string, int, error) {
	prefix := 1
	prev := uint32(data[0])
	if prev == 0xfe {
		if len(data) < 5 {
			return "", 0, errPackedHash
		}
		prefix = 5
		prev = binary.LittleEndian.Uint32(data[1:])
	}
	if data[0] == 0xff || uint64(prev) != uint64(previous) || len(data) <= prefix {
		return "", 0, errPackedHash
	}
	data = data[prefix:]
	tag := data[0]
	var value string
	var size int
	var err error
	switch {
	case tag < 0x40:
		value, size, err = packedString(data, 1, uint32(tag))
	case tag < 0x80 && len(data) >= 2:
		value, size, err = packedString(data, 2, uint32(tag&0x3f)<<8|uint32(data[1]))
	case tag == 0x80 && len(data) >= 5:
		value, size, err = packedString(data, 5, binary.BigEndian.Uint32(data[1:]))
	case tag >= 0xf1 && tag <= 0xfd:
		value, size = strconv.Itoa(int(tag-0xf1)), 1
	case tag == 0xc0:
		value, size, err = packedInteger(data, 2)
	case tag == 0xd0:
		value, size, err = packedInteger(data, 4)
	case tag == 0xe0:
		value, size, err = packedInteger(data, 8)
	case tag == 0xf0:
		value, size, err = packedInteger(data, 3)
	case tag == 0xfe:
		value, size, err = packedInteger(data, 1)
	default:
		return "", 0, errPackedHash
	}
	return value, prefix + size, err
}

func packedString(data []byte, header int, length uint32) (string, int, error) {
	if uint64(length) > uint64(len(data)-header) {
		return "", 0, errPackedHash
	}
	size := header + int(length)
	return string(data[header:size]), size, nil
}

// 두 형식 모두 정수의 본문은 리틀 엔디언이다. 24비트를 포함해 저장된 폭에
// 맞춰 부호를 확장한 뒤, HGET이 돌려줄 원래의 10진 문자열로 바꾼다.
func packedInteger(data []byte, width int) (string, int, error) {
	if len(data)-1 < width {
		return "", 0, errPackedHash
	}
	var n uint64
	for i, b := range data[1 : 1+width] {
		n |= uint64(b) << (8 * i)
	}
	shift := 64 - 8*width
	return strconv.FormatInt(int64(n<<shift)>>shift, 10), width + 1, nil
}

// LZF는 짧은 원문 바이트 또는 앞서 복원한 구간의 위치와 길이를 저장한다.
// 같은 구간을 반복하는 참조는 읽는 위치와 쓰는 위치가 겹칠 수 있다.
func decodeLZF(src []byte, outputLen uint64) ([]byte, error) {
	invalid := errors.New("invalid RDB LZF string")
	if outputLen > math.MaxInt {
		return nil, invalid
	}
	out := make([]byte, 0, min(int(outputLen), len(src)))
	for len(src) > 0 {
		ctrl := int(src[0])
		src = src[1:]
		if ctrl < 32 {
			n := ctrl + 1
			if n > len(src) || n > int(outputLen)-len(out) {
				return nil, invalid
			}
			out = append(out, src[:n]...)
			src = src[n:]
			continue
		}
		n := ctrl >> 5
		if n == 7 {
			if len(src) == 0 {
				return nil, invalid
			}
			n += int(src[0])
			src = src[1:]
		}
		n += 2
		if len(src) == 0 || n > int(outputLen)-len(out) {
			return nil, invalid
		}
		distance := ((ctrl & 0x1f) << 8) + int(src[0]) + 1
		src = src[1:]
		if distance > len(out) {
			return nil, invalid
		}
		for range n {
			out = append(out, out[len(out)-distance])
		}
	}
	if len(out) != int(outputLen) {
		return nil, invalid
	}
	return out, nil
}
