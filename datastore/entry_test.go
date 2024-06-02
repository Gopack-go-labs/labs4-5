package datastore

import (
	"bufio"
	"bytes"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_EntryString(t *testing.T) {
	t.Run("Encode", func(t *testing.T) {
		e := entry{"key", "value", Str}
		e.Decode(e.Encode())
		assert.Equal(t, Str, e.valueType)
		assert.Equal(t, "key", e.key)
		assert.Equal(t, "value", e.value)
	})

	t.Run("Decode", func(t *testing.T) {
		e := entry{"key", "test-value", Str}
		data := e.Encode()
		v, err := readValue(bufio.NewReader(bytes.NewReader(data)))
		s, ok := v.(string)
		assert.True(t, ok)
		assert.Nil(t, err)
		assert.Equal(t, "test-value", s)
	})
}

func Test_EntryInt64(t *testing.T) {
	t.Run("Encode", func(t *testing.T) {
		val := int64(123)
		e := entry{"key", val, Int}
		e.Decode(e.Encode())
		assert.Equal(t, Int, e.valueType)
		assert.Equal(t, "key", e.key)
		assert.Equal(t, val, e.value)
	})

	t.Run("Encode negative", func(t *testing.T) {
		val := int64(-123)
		e := entry{"key", val, Int}
		e.Decode(e.Encode())
		assert.Equal(t, Int, e.valueType)
		assert.Equal(t, "key", e.key)
		assert.Equal(t, val, e.value)
	})

	t.Run("Decode", func(t *testing.T) {
		e := entry{"key", int64(123), Int}
		data := e.Encode()
		v, err := readValue(bufio.NewReader(bytes.NewReader(data)))
		s, ok := v.(int64)
		assert.True(t, ok)
		assert.Nil(t, err)
		assert.Equal(t, int64(123), s)
	})
}
