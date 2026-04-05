package errdefs

import (
	"errors"
	"fmt"
)

var (
	ErrInvalidArgument = errors.New("invalid argument")
	ErrNotFound        = errors.New("not found")
	ErrConflict        = errors.New("conflict")
)

func InvalidArgumentf(format string, args ...any) error {
	return wrapf(ErrInvalidArgument, format, args...)
}

func NotFoundf(format string, args ...any) error {
	return wrapf(ErrNotFound, format, args...)
}

func Conflictf(format string, args ...any) error {
	return wrapf(ErrConflict, format, args...)
}

func wrapf(base error, format string, args ...any) error {
	all := make([]any, 0, len(args)+1)
	all = append(all, base)
	all = append(all, args...)
	return fmt.Errorf("%w: "+format, all...)
}
