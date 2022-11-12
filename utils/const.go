package utils

import (
	"hash/crc32"
	"os"
)

// file
const (
	ManifestFilename                  = "MANIFEST"
	ManifestRewriteFilename           = "REWRITEMANIFEST"
	ManifestDeletionsRewriteThreshold = 10000
	ManifestDeletionsRatio            = 10

	DefaultFileFlag = os.O_RDWR | os.O_CREATE | os.O_APPEND
	DefaultFileMode = 0666
)

// codec
var (
	MagicText          = [4]byte{'j', 'v', 'v', 'v'}
	MagicVersion       = uint32(1)
	CastagnoliCrcTable = crc32.MakeTable(crc32.Castagnoli) // CastagnoliCrcTable is a CRC32 polynomial table
)

// meta
const (
	BitDelete       byte = 1 << 0 // Set if the key has been deleted.
	BitValuePointer byte = 1 << 1 // Set if the value is NOT stored directly next to key.
)
