package utils

import "math"

// Filter is an encoded set of []byte keys.
type Filter []byte

func (f Filter) MayContainKey(k []byte) bool {
	return f.MayContain(Hash(k))
}

// MayContain returns whether the filter may contain given key.
// False positives are possible, where it returns true for keys
// not in the original set.
func (f Filter) MayContain(h uint32) bool {
	if len(f) < 2 {
		return false
	}

	k := f[len(f)-1] // amount of hash func stores in last position
	if k > 30 {
		// reserved
		// consider it a match
		return true
	}

	nBits := uint32(8 * (len(f) - 1))

	delta := h>>17 | h<<15
	for j := uint8(0); j < k; j++ {
		bitPos := h % nBits
		if f[bitPos/8]&(1<<(bitPos%8)) == 0 {
			return false
		}
		h += delta
	}

	return true
}

// NewFilter returns a bew BloomFilter that encodes a set of []byte keys with
// the given number of bits per key, approximately.
//
// A good bitsPerKey value is 10, which yields a filter with ~ 1% FP rate
func NewFilter(keys []uint32, bitsPerKey int) Filter {
	return Filter(appendFilter(keys, bitsPerKey))
}

// BloomBitsPerKey returns the bits per key required by bloomfilter based on
// the false positive rate
func BloomBitsPerKey(numEntries int, fp float64) int {
	size := -1 * float64(numEntries) * math.Log(fp) / math.Pow(float64(0.69314718056), 2)
	locs := math.Ceil(size / float64(numEntries))
	return int(locs)
}

func appendFilter(keys []uint32, bitsPerKey int) []byte {
	if bitsPerKey < 0 {
		bitsPerKey = 0
	}

	// 0.69 is approximately ln(2)
	k := uint32(0.69 * float64(bitsPerKey))
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}

	nBits := len(keys) * int(bitsPerKey)

	if nBits < 64 {
		nBits = 64
	}
	// get the multiple of 8
	nBytes := (nBits + 7) / 8
	nBits = 8 * nBytes

	// amount of hash functions stores in the last position
	filter := make([]byte, nBytes+1)
	filter[nBytes] = uint8(k)

	for _, h := range keys {
		// not really craete k hash functions
		// convert h to delta, add to h to get hash
		delta := h>>17 | h<<15
		for j := uint32(0); j < k; j++ {
			bitPos := h % uint32(nBits)
			filter[bitPos/8] |= 1 << (bitPos % 8)
			h += delta
		}
	}

	return filter
}

// Hash implements a hashing algorithm to the MurmurHash
func Hash(b []byte) uint32 {
	const (
		seed = 0xbc9f1d34
		m    = 0xc6a4a793
	)
	h := uint32(seed) ^ uint32(len(b))*m
	for ; len(b) >= 4; b = b[4:] {
		h += uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
		h *= m
		h ^= h >> 16
	}

	switch len(b) {
	case 3:
		h += uint32(b[2]) << 16
		fallthrough
	case 2:
		h += uint32(b[1]) << 8
		fallthrough
	case 1:
		h += uint32(b[0])
		h *= m
		h ^= h >> 24
	}
	return h
}
