package bolt

import (
	"encoding/binary"
)

func ub(i uint64) (r []byte) {
	r = make([]byte, 8)
	binary.LittleEndian.PutUint64(r, i)
	return
}

func bu(b []byte) (i uint64) {
	if b == nil {
		return 0
	}
	return binary.LittleEndian.Uint64(b)
}

//int64
func ib64(i int64) (r []byte) {
	r = make([]byte, 8)
	binary.LittleEndian.PutUint64(r, uint64(i))
	return
}

func bi64(b []byte) (i int64) {
	if b == nil {
		return 0
	}
	return int64(binary.LittleEndian.Uint64(b))
}

//int
func ib(i int) (r []byte) {
	r = make([]byte, 8)
	binary.LittleEndian.PutUint64(r, uint64(i))
	return
}

func bi(b []byte) (i int) {
	if b == nil {
		return 0
	}
	return int(binary.LittleEndian.Uint64(b))
}
