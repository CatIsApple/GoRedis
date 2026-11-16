package persist

import (
	"os"
	"syscall"
	"unsafe"
)

var moveFileEx = syscall.NewLazyDLL("kernel32.dll").NewProc("MoveFileExW")

// Windows에서는 같은 볼륨 안의 manifest 교체에 write-through를 요청한다.
func replaceManifest(from, to string) error {
	src, err := syscall.UTF16PtrFromString(from)
	if err != nil {
		return err
	}
	dst, err := syscall.UTF16PtrFromString(to)
	if err != nil {
		return err
	}
	const replaceExisting, writeThrough = 0x1, 0x8
	ok, _, moveErr := moveFileEx.Call(uintptr(unsafe.Pointer(src)), uintptr(unsafe.Pointer(dst)), replaceExisting|writeThrough)
	if ok == 0 {
		return &os.LinkError{Op: "replace manifest", Old: from, New: to, Err: moveErr}
	}
	return nil
}

// 디렉터리 핸들에는 일반 파일용 FlushFileBuffers를 사용할 수 없다.
// 파일은 교체 전에 각각 Sync하고 이름 교체는 위 MoveFileExW에서 기다린다.
func syncDir(string) error { return nil }
