package utils

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
)

var (
	gopath = path.Join(os.Getenv("GOPATH"), "src") + "/"
)

// NotFoundKey 找不到key
var (
	ErrKeyNotFound    = errors.New("Key not found")       // ErrKeyNotFound is returned when key isn't found on a txn.Get.
	ErrEmptyKey       = errors.New("Key cannot be empty") // ErrEmptyKey is returned if an empty key is passed on an update function.
	ErrRewriteFailure = errors.New("rewrite failure")     // ErrRewriteFailure rewrite failure

	ErrBadMagic         = errors.New("bad magic")         // ErrBadMagic bad magic
	ErrChecksumMismatch = errors.New("checksum mismatch") // ErrChecksumMismatch is returned at checksum mismatch.)

	ErrTruncate = errors.New("Do truncate")
	ErrStop     = errors.New("Stop")

	// compact
	ErrFillTables = errors.New("Unable to fill tables")
)

// Panic 如果err不为nil 则panic
func Panic(err error) {
	if err != nil {
		panic(err)
	}
}

// Panic2 _
func Panic2(_ interface{}, err error) {
	Panic(err)
}

// Err err
func Err(err error) error {
	if err != nil {
		fmt.Printf("%s %s\n", location(2, true), err)
	}
	return err
}

// WarpErr err
func WarpErr(format string, err error) error {
	if err != nil {
		fmt.Printf("%s %s %s", format, location(2, true), err)
	}
	return err
}

func location(deep int, fullPath bool) string {
	_, file, line, ok := runtime.Caller(deep)
	if !ok {
		file = "???"
		line = 0
	}
	if fullPath {
		if strings.HasPrefix(file, gopath) {
			file = file[len(gopath):]
		}
	} else {
		file = filepath.Base(file)
	}
	return file + ":" + strconv.Itoa(line)
}

// CondPanic e
func CondPanic(condition bool, err error) {
	if condition {
		Panic(err)
	}
}
