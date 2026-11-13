//go:build darwin || dragonfly || freebsd || illumos || linux || netbsd || openbsd

package persist

import (
	"os"
	"syscall"
)

// 서버 두 개가 같은 로그를 동시에 복구하거나 갱신하는 것을 막는다.
// 파일 디스크립터를 닫거나 프로세스가 종료되면 커널이 잠금을 해제한다.
func openLockedFile(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, &os.PathError{Op: "lock AOF", Path: path, Err: err}
	}
	return f, nil
}
