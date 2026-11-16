//go:build !darwin && !dragonfly && !freebsd && !illumos && !linux && !netbsd && !openbsd && !windows

package persist

import (
	"errors"
	"os"
)

func openLockedFile(path string) (*os.File, error) {
	return nil, &os.PathError{Op: "lock AOF", Path: path, Err: errors.ErrUnsupported}
}
