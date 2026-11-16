package resp

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
)

// Reader는 같은 입력에서 이어지는 RESP2 값을 차례로 읽는다.
// TCP에서 한 요청이 여러 번에 나뉘어 도착하면 부족한 바이트를 더 읽는다.
// 여러 요청이 한꺼번에 도착하면 미리 읽은 다음 요청의 바이트가 버퍼에 남으므로 연결마다 Reader 하나를 재사용한다.
type Reader struct {
	input *bufio.Reader
}

func NewReader(input io.Reader) *Reader {
	return &Reader{input: bufio.NewReader(input)}
}

// Buffered는 입력에서 미리 읽었지만 아직 소비하지 않은 바이트 수를 반환한다.
// AOF 복구 시 파일에서 읽은 위치에서 이 수를 빼면 실제로 해석한 레코드의 끝을 알 수 있다.
func (r *Reader) Buffered() int {
	return r.input.Buffered()
}

// ReadValue는 값 사이의 EOF와 값 도중의 io.ErrUnexpectedEOF를 구분한다.
// 프로토콜 오류가 발생하면 현재 메시지의 다음 경계를 알 수 없다.
func (r *Reader) ReadValue() (Value, error) {
	return r.readValue(0)
}

func (r *Reader) readValue(depth int) (Value, error) {
	prefix, err := r.input.ReadByte()
	if err != nil {
		if depth > 0 {
			err = frameError(err)
		}
		return Value{}, err
	}

	kind := Kind(prefix)
	switch kind {
	case SimpleString, SimpleError, Integer, BulkString:
	case Array:
		if depth >= maxDepth {
			return Value{}, fmt.Errorf("%w: array nesting exceeds %d", ErrProtocol, maxDepth)
		}
	default:
		return Value{}, fmt.Errorf("%w: unknown type byte %#x", ErrProtocol, prefix)
	}
	// 접두 바이트 다음 줄의 의미는 자료형에 따라 달라진다.
	// +OK에서는 줄 자체가 값이고, $3에서는 뒤에 읽을 길이, *3에서는 원소 개수다.
	line, err := r.readLine()
	if err != nil {
		return Value{}, err
	}
	switch kind {
	case SimpleString, SimpleError:
		// line이 Reader의 내부 버퍼를 가리킬 수 있어 다음 읽기 전에 값의 바이트를 복사한다.
		return Value{Kind: kind, Bytes: bytes.Clone(line)}, nil
	case Integer:
		n, err := strconv.ParseInt(string(line), 10, 64)
		if err != nil {
			return Value{}, fmt.Errorf("%w: invalid integer", ErrProtocol)
		}
		return Value{Kind: kind, Int: n}, nil
	case BulkString:
		n, err := parseLength(line, maxBulkBytes)
		if err != nil {
			return Value{}, err
		}
		if n < 0 {
			return Value{Kind: kind}, nil
		}
		return r.readBulk(n)
	default: // Array
		n, err := parseLength(line, maxArrayLen)
		if err != nil {
			return Value{}, err
		}
		if n < 0 {
			return Value{Kind: kind}, nil
		}
		// 원소마다 자체 접두 바이트가 있다. 같은 Reader에서 재귀 호출하면 배열 안의 배열도 읽을 수 있다.
		elements := make([]Value, 0, min(n, 16))
		for range n {
			element, err := r.readValue(depth + 1)
			if err != nil {
				return Value{}, err
			}
			elements = append(elements, element)
		}
		return Value{Kind: kind, Elements: elements}, nil
	}
}

// readLine의 결과는 다음 버퍼 읽기 전까지만 유효하다.
func (r *Reader) readLine() ([]byte, error) {
	var line []byte
	for {
		part, err := r.input.ReadSlice('\n')
		if len(line)+len(part) > maxLineBytes+2 {
			return nil, fmt.Errorf("%w: line exceeds %d bytes", ErrProtocol, maxLineBytes)
		}
		if err == bufio.ErrBufferFull {
			// 줄 끝을 만나기 전에 버퍼가 찼다. 다음 읽기가 덮어쓰기 전에 현재 조각을 보관한다.
			line = append(line, part...)
			continue
		}
		if err != nil {
			return nil, frameError(err)
		}
		if line == nil {
			line = part
		} else {
			line = append(line, part...)
		}
		if len(line) < 2 || line[len(line)-2] != '\r' {
			return nil, fmt.Errorf("%w: line must end in CRLF", ErrProtocol)
		}
		line = line[:len(line)-2]
		if bytes.IndexByte(line, '\r') >= 0 {
			return nil, fmt.Errorf("%w: unexpected carriage return in line", ErrProtocol)
		}
		return line, nil
	}
}

func parseLength(line []byte, limit int) (int, error) {
	if string(line) == "-1" {
		return -1, nil
	}
	n, err := strconv.ParseUint(string(line), 10, 64)
	if err != nil || n > uint64(limit) {
		return 0, fmt.Errorf("%w: invalid length (maximum %d)", ErrProtocol, limit)
	}
	return int(n), nil
}

// readBulk는 본문을 정확히 n바이트 읽은 뒤, 그 다음 두 바이트가 CRLF인지 확인한다.
// 본문 안의 줄바꿈이나 NUL 바이트도 값의 일부이므로 그대로 보존한다.
func (r *Reader) readBulk(n int) (Value, error) {
	var payload []byte
	if n <= r.input.Size() {
		payload = make([]byte, n)
		if _, err := io.ReadFull(r.input, payload); err != nil {
			return Value{}, frameError(err)
		}
	} else {
		// 큰 길이 선언만으로 전체 메모리를 할당하지 않고 도착한 만큼 늘린다.
		var err error
		payload, err = io.ReadAll(io.LimitReader(r.input, int64(n)))
		if err != nil {
			return Value{}, err
		}
		if len(payload) != n {
			return Value{}, io.ErrUnexpectedEOF
		}
	}
	for _, want := range []byte{'\r', '\n'} {
		got, err := r.input.ReadByte()
		if err != nil {
			return Value{}, frameError(err)
		}
		if got != want {
			return Value{}, fmt.Errorf("%w: bulk string must end in CRLF", ErrProtocol)
		}
	}
	return Value{Kind: BulkString, Bytes: payload}, nil
}

func frameError(err error) error {
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	return err
}
