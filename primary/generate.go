package primary

import (
	"bytes"
	cr "crypto/rand"
	"encoding/binary"
)

func GenerateId() uint64 {
	var id uint64
	var b [8]byte
	cr.Read(b[:])
	binary.Read(bytes.NewReader(b[:]), binary.BigEndian, &id)
	return id
}
