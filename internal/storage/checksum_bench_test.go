package storage

import (
	"crypto/sha256"
	"hash/crc32"
	"testing"
)

// These isolate the cost of the two integrity mechanisms the engine runs on
// every byte: SHA-256 (end-to-end object identity) and CRC32C per block
// (cheap, block-local corruption detection on reads). See docs/performance.md.

func BenchmarkSHA256_1MiB(b *testing.B) {
	data := randBytes(1<<20, 1)
	b.SetBytes(int64(len(data)))
	for i := 0; i < b.N; i++ {
		sha256.Sum256(data)
	}
}

func BenchmarkCRC32C_1MiB(b *testing.B) {
	data := randBytes(1<<20, 1)
	b.SetBytes(int64(len(data)))
	for i := 0; i < b.N; i++ {
		crc32.Checksum(data, castagnoli)
	}
}
