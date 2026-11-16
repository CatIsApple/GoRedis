package persist

import (
	"os"
	"syscall"
)

// Windows에서는 읽기 공유만 허용해 다른 프로세스가 같은 AOF를 쓰기용으로
// 열 수 없도록 한다. OPEN_ALWAYS는 기존 파일을 보존하고 없는 파일만 만든다.
func openLockedFile(path string) (*os.File, error) {
	name, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}
	handle, err := syscall.CreateFile(name, syscall.GENERIC_READ|syscall.GENERIC_WRITE,
		syscall.FILE_SHARE_READ, nil, syscall.OPEN_ALWAYS, syscall.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		return nil, &os.PathError{Op: "open AOF", Path: path, Err: err}
	}
	return os.NewFile(uintptr(handle), path), nil
}
