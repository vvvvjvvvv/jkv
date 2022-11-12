package utils

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

// FID 根据file name 获取其fid
func FID(name string) uint64 {
	name = path.Base(name)
	if !strings.HasSuffix(name, ".sst") {
		return 0
	}
	//	suffix := name[len(fileSuffix):]
	name = strings.TrimSuffix(name, ".sst")
	id, err := strconv.Atoi(name)
	if err != nil {
		Err(err)
		return 0
	}
	return uint64(id)
}

// FileNameSSTable  sst 文件名
func FileNameSSTable(dir string, id uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%05d.sst", id))
}

// openDir opens a directory ofr syncing.
func openDir(path string) (*os.File, error) {
	return os.Open(path)
}

// SyncDir When you create or delete a file, you want to ensure the directory entry for the file is synced
// in order to guarantee the file is visible (if the system crashes). (See the man page for fsync,
// or see https://github.com/coreos/etcd/issues/6368 for an example.)
func SyncDir(dir string) error {
	f, err := openDir(dir)
	if err != nil {
		return errors.Wrapf(err, "While open directory: %s.", dir)
	}

	err = f.Sync()
	if err != nil {
		return errors.Wrapf(err, "While sync directory: %s.", dir)
	}

	closeErr := f.Close()
	return errors.Wrapf(closeErr, "While close directory: %s.", dir)
}

// LoadIDMap Get the id of all sst files in the current folder
func LoadIDMap(dir string) map[uint64]struct{} {
	fileInfos, err := ioutil.ReadDir(dir)
	Err(err)
	idMap := make(map[uint64]struct{})
	for _, info := range fileInfos {
		if info.IsDir() {
			continue
		}
		fileID := FID(info.Name())
		if fileID != 0 {
			idMap[fileID] = struct{}{}
		}
	}
	return idMap
}

// CompareKyes checks the key without timestamp and checks the timestamp if keyNoTs
// is same
// a<timestamp> would be sorted higher than aa<timestamp> if we use bytes.compare
// All keys should have timestamp
func CompareKeys(key1, key2 []byte) int {
	CondPanic((len(key1) <= 8 || len(key2) <= 8), fmt.Errorf("%s, %s < 8", string(key1), string(key2)))
	if cmp := bytes.Compare(key1[:len(key1)-8], key2[:len(key2)-8]); cmp != 0 {
		return cmp
	}
	return bytes.Compare(key1[len(key1)-8:], key2[len(key2)-8:])
}

// VerifyChecksum crc32
func VerifyChecksum(data []byte, expected []byte) error {
	actual := uint64(crc32.Checksum(data, CastagnoliCrcTable))
	expectedU64 := BytesToU64(expected)
	if actual != expectedU64 {
		return errors.Wrapf(ErrChecksumMismatch, "actual: %d, expected: %d", actual, expectedU64)
	}

	return nil
}

// CalculateChecksum _
func CalculateChecksum(data []byte) uint64 {
	return uint64(crc32.Checksum(data, CastagnoliCrcTable))
}
