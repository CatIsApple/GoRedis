package resp

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
)

// Writer는 RESP2 값을 버퍼에 기록한다. 응답을 보내려면 Flush를 호출한다.
type Writer struct {
	buf *bufio.Writer
}

func NewWriter(dst io.Writer) *Writer {
	return &Writer{buf: bufio.NewWriter(dst)}
}

// WriteValue는 중첩 값까지 검증한 뒤 하나의 RESP2 메시지를 기록한다.
// 배열의 마지막 원소에 형식 오류가 있어도 앞 원소들만 먼저 기록되는 일을 막는다.
func (w *Writer) WriteValue(v Value) error {
	if err := validateValue(v, 0); err != nil {
		return err
	}
	return w.writeValue(v)
}

// Flush는 남은 버퍼를 연결이나 파일에 전달한다. 파일의 디스크 동기화는 AOF가 따로 담당한다.
func (w *Writer) Flush() error {
	return w.buf.Flush()
}

func validateValue(v Value, depth int) error {
	switch v.Kind {
	case SimpleString, SimpleError:
		if len(v.Bytes) > maxLineBytes || bytes.ContainsAny(v.Bytes, "\r\n") {
			return fmt.Errorf("%w: invalid simple string or error", ErrProtocol)
		}
	case Integer:
	case BulkString:
		if len(v.Bytes) > maxBulkBytes {
			return fmt.Errorf("%w: bulk string too long", ErrProtocol)
		}
	case Array:
		if depth >= maxDepth || len(v.Elements) > maxArrayLen {
			return fmt.Errorf("%w: array exceeds size or nesting limit", ErrProtocol)
		}
		for _, child := range v.Elements {
			if err := validateValue(child, depth+1); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("%w: unknown RESP type %q", ErrProtocol, byte(v.Kind))
	}
	return nil
}

func (w *Writer) writeValue(v Value) error {
	switch v.Kind {
	case Integer:
		return w.writeNumber(v.Kind, v.Int)
	case Array:
		if v.Elements == nil {
			return w.writeNumber(Array, -1)
		}
		if err := w.writeNumber(Array, int64(len(v.Elements))); err != nil {
			return err
		}
		for _, child := range v.Elements {
			if err := w.writeValue(child); err != nil {
				return err
			}
		}
		return nil
	case BulkString:
		if v.Bytes == nil {
			return w.writeNumber(BulkString, -1)
		}
		// 먼저 $<바이트 수>와 CRLF를 쓰고, 아래의 공통 코드에서 본문과 마지막 CRLF를 쓴다.
		if err := w.writeNumber(BulkString, int64(len(v.Bytes))); err != nil {
			return err
		}
	case SimpleString, SimpleError:
		if err := w.buf.WriteByte(byte(v.Kind)); err != nil {
			return err
		}
	}
	if _, err := w.buf.Write(v.Bytes); err != nil {
		return err
	}
	_, err := w.buf.WriteString("\r\n")
	return err
}

func (w *Writer) writeNumber(kind Kind, n int64) error {
	line := append(w.buf.AvailableBuffer(), byte(kind))
	line = strconv.AppendInt(line, n, 10)
	line = append(line, '\r', '\n')
	_, err := w.buf.Write(line)
	return err
}
