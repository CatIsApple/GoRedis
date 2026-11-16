//go:build !windows

package persist

import "os"

func replaceManifest(from, to string) error {
	return os.Rename(from, to)
}

// rename 성공 후 디렉터리 항목까지 동기화해야 이전 세대 파일을 지울 수 있다.
func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
