//go:build !(linux || darwin || freebsd)

package storage

func fsFree(string) int64 { return 0 }
