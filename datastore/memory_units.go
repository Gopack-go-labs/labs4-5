package datastore

type MemoryUnit int64

func (m MemoryUnit) Bytes() int64 {
	return int64(m / Byte)
}

const (
	Bit      MemoryUnit = 1
	Byte                = 8 * Bit
	Kilobyte            = 1024 * Byte
	Megabyte            = 1024 * Kilobyte
	Gigabyte            = 1024 * Megabyte
)
