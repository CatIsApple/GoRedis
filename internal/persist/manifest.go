package persist

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// base는 스냅샷 한 개, tails는 그 뒤에 차례로 재생할 증분 AOF들이다.
// 재작성 도중에는 이전 base와 둘 이상의 tail이 동시에 필요하다.
type part struct {
	name string
	seq  int64
}

type manifest struct {
	base  part
	tails []part
}

func readManifest(path string) (manifest, error) {
	f, err := os.Open(path)
	if err != nil {
		return manifest{}, err
	}
	defer f.Close()

	var m manifest
	seen := make(map[string]bool)
	scanner := bufio.NewScanner(f)
	scanner.Split(func(data []byte, eof bool) (int, []byte, error) {
		if i := bytes.IndexByte(data, '\n'); i >= 0 {
			return i + 1, data[:i], nil
		}
		if eof && len(data) > 0 {
			return 0, nil, io.ErrUnexpectedEOF
		}
		return 0, nil, nil
	})
	for line := 1; scanner.Scan(); line++ {
		s := scanner.Text()
		if strings.HasPrefix(s, "#") {
			continue
		}
		args, err := manifestWords(s)
		if err != nil || len(args) < 6 || len(args)%2 != 0 {
			return manifest{}, fmt.Errorf("invalid AOF manifest at line %d", line)
		}
		// Redis 8은 startoffset 등 추가 속성을 기록한다. 파일·순번·종류를 읽고
		// 그 외의 속성은 재생 순서를 바꾸지 않으므로 건너뛴다.
		var p part
		var kind string
		for i := 0; i < len(args); i += 2 {
			switch strings.ToLower(args[i]) {
			case "file":
				p.name = args[i+1]
			case "seq":
				p.seq, err = strconv.ParseInt(args[i+1], 10, 64)
				if err != nil {
					return manifest{}, fmt.Errorf("invalid AOF sequence at line %d", line)
				}
			case "type":
				kind = args[i+1]
			}
		}
		if p.seq < 1 || !filepath.IsLocal(p.name) || filepath.Base(p.name) != p.name ||
			p.name == "." || p.name == filepath.Base(path) || p.name == ".goredis-lock" || p.name == initMarker || seen[p.name] {
			return manifest{}, fmt.Errorf("invalid AOF part at line %d", line)
		}
		seen[p.name] = true
		switch kind {
		case "b":
			if m.base.name != "" {
				return manifest{}, errors.New("duplicate AOF base")
			}
			m.base = p
		case "i":
			if n := len(m.tails); n > 0 && p.seq <= m.tails[n-1].seq {
				return manifest{}, errors.New("out-of-order AOF increment")
			}
			m.tails = append(m.tails, p)
		case "h":
			// Redis가 이전 세대 삭제를 예약하면서 남기는 history는 재생하지 않는다.
		default:
			return manifest{}, fmt.Errorf("unknown AOF part type %q", kind)
		}
	}
	if err := scanner.Err(); err != nil {
		return manifest{}, err
	}
	if m.base.name == "" && len(m.tails) == 0 {
		return manifest{}, errors.New("AOF manifest has no active files")
	}
	return m, nil
}

// Redis는 공백 등이 들어간 파일명을 이중 따옴표와 역슬래시로 이스케이프한다.
func manifestWords(s string) ([]string, error) {
	var words []string
	for {
		s = strings.TrimLeft(s, " \t\r")
		if s == "" {
			return words, nil
		}
		if s[0] != '"' {
			end := strings.IndexAny(s, " \t\r")
			if end == -1 {
				return append(words, s), nil
			}
			words = append(words, s[:end])
			s = s[end:]
			continue
		}
		end, err := strconv.QuotedPrefix(s)
		if err != nil {
			return nil, err
		}
		word, err := strconv.Unquote(end)
		if err != nil {
			return nil, err
		}
		words = append(words, word)
		s = s[len(end):]
	}
}

func manifestName(s string) string {
	if strings.IndexFunc(s, func(r rune) bool { return r < '!' || r > '~' || r == '"' || r == '\'' || r == '\\' }) < 0 {
		return s
	}
	// Redis는 임의 바이트를 \xNN으로 복원한다. Go 전용 유니코드 이스케이프를 쓰지 않는다.
	var b strings.Builder
	b.WriteByte('"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '\\' || c == '"':
			b.WriteByte('\\')
			b.WriteByte(c)
		case c < ' ' || c > '~':
			fmt.Fprintf(&b, "\\x%02x", c)
		default:
			b.WriteByte(c)
		}
	}
	b.WriteByte('"')
	return b.String()
}

// 먼저 임시 파일을 디스크에 내리고 rename으로 교체한다. renamed가 true인 뒤
// 디렉터리 동기화가 실패하면 어느 세대로 부팅하더라도 되도록 양쪽 파일을 모두 보존한다.
func installManifest(path string, m manifest) (renamed bool, err error) {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".goredis-manifest-*")
	if err != nil {
		return false, err
	}
	defer os.Remove(tmp.Name())
	write := func(p part, kind byte) {
		if err == nil {
			_, err = fmt.Fprintf(tmp, "file %s seq %d type %c\n", manifestName(p.name), p.seq, kind)
		}
	}
	if m.base.name != "" {
		write(m.base, 'b')
	}
	for _, p := range m.tails {
		write(p, 'i')
	}
	if err == nil {
		err = tmp.Sync()
	}
	err = errors.Join(err, tmp.Close())
	if err != nil {
		return false, err
	}
	if err := replaceManifest(tmp.Name(), path); err != nil {
		return false, err
	}
	return true, syncDir(dir)
}

// 같은 디렉터리에 남은 미완성 세대와 이름이 겹쳐도 기존 파일을 덮어쓰지 않는다.
func newPart(dir, prefix, suffix string, last int64) (part, *os.File, error) {
	for seq := last + 1; seq > 0; seq++ {
		p := part{name: fmt.Sprintf("%s.%d.%s", prefix, seq, suffix), seq: seq}
		f, err := os.OpenFile(filepath.Join(dir, p.name), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
		if errors.Is(err, os.ErrExist) {
			continue
		}
		return p, f, err
	}
	return part{}, nil, errors.New("AOF file sequence exhausted")
}
