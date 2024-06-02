package datastore

import (
	"bufio"
	"encoding/binary"
	"fmt"
)

const (
	Str = iota
	Int
)

type entry struct {
	key       string
	value     interface{}
	valueType int
}

func (e *entry) Encode() []byte {
	kl := len(e.key)
	var vl int
	if e.valueType == Int {
		vl = 8
	} else {
		vl = len(e.value.(string))
	}
	size := kl + vl + 13
	res := make([]byte, size)
	binary.LittleEndian.PutUint32(res, uint32(size))
	binary.LittleEndian.PutUint32(res[4:], uint32(kl))
	copy(res[8:], e.key)

	if _, ok := e.value.(int64); ok {
		res[kl+8] = Int
	} else {
		res[kl+8] = Str
	}

	binary.LittleEndian.PutUint32(res[kl+9:], uint32(vl))
	if e.valueType == Int {
		binary.LittleEndian.PutUint64(res[kl+13:], uint64(e.value.(int64)))
	} else {
		v := e.value.(string)
		copy(res[kl+13:], v)
	}

	return res
}

func (e *entry) Size() MemoryUnit {
	if e.valueType == Int {
		return MemoryUnit((len(e.key) + 13 + 8) * 8)
	}
	bytes := len(e.key) + len(e.value.(string)) + 13
	return MemoryUnit(bytes * 8)
}

func (e *entry) Decode(input []byte) {
	typeBuf := make([]byte, 4)
	copy(typeBuf, input[:4])

	kl := binary.LittleEndian.Uint32(input[4:])
	keyBuf := make([]byte, kl)
	copy(keyBuf, input[8:kl+8])
	e.key = string(keyBuf)

	typeFlag := input[kl+8]

	vl := binary.LittleEndian.Uint32(input[kl+9:])

	if typeFlag == Int {
		e.valueType = Int
		val := binary.LittleEndian.Uint64(input[kl+13 : kl+13+vl])
		e.value = int64(val)
	} else {
		e.valueType = Str
		valBuf := make([]byte, vl)
		copy(valBuf, input[kl+13:kl+13+vl])
		e.value = string(valBuf)
	}
}

func readValue(in *bufio.Reader) (interface{}, error) {
	header, err := in.Peek(8)
	if err != nil {
		return "", err
	}
	keySize := int(binary.LittleEndian.Uint32(header[4:]))
	_, err = in.Discard(keySize + 8)
	if err != nil {
		return "", err
	}

	typeFlag, err := in.ReadByte()
	if err != nil {
		return "", err
	}

	header, err = in.Peek(4)
	if err != nil {
		return "", err
	}
	valSize := int(binary.LittleEndian.Uint32(header))
	_, err = in.Discard(4)
	if err != nil {
		return "", err
	}

	if typeFlag == Int {
		header, err = in.Peek(8)
		if err != nil {
			return 0, err
		}
		return int64(binary.LittleEndian.Uint64(header)), nil
	} else {
		data := make([]byte, valSize)
		n, err := in.Read(data)
		if err != nil {
			return "", err
		}
		if n != valSize {
			return "", fmt.Errorf("can't read value bytes (read %d, expected %d)", n, valSize)
		}
		return string(data), nil
	}
}
