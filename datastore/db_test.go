package datastore

import (
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func TestDb_Put(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDb(dir, 10*Megabyte)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	pairs := [][]string{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
	}

	outFile := db.curSegment().file
	if err != nil {
		t.Fatal(err)
	}

	t.Run("put/get", func(t *testing.T) {
		for _, pair := range pairs {
			err := db.PutString(pair[0], pair[1])
			assert.Nil(t, err)
			value, err := db.GetString(pair[0])
			assert.Nil(t, err)
			assert.Equal(t, pair[1], value)
		}
	})

	outInfo, err := outFile.Stat()
	if err != nil {
		t.Fatal(err)
	}
	size1 := outInfo.Size()

	t.Run("file growth", func(t *testing.T) {
		for _, pair := range pairs {
			err := db.PutString(pair[0], pair[1])
			assert.Nil(t, err)
		}
		outInfo, err := outFile.Stat()
		assert.Nil(t, err)
		assert.Equal(t, size1*2, outInfo.Size())
	})

	t.Run("new db process", func(t *testing.T) {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db, err = NewDb(dir, 10*Megabyte)
		assert.Nil(t, err)

		for _, pair := range pairs {
			value, err := db.GetString(pair[0])
			assert.Nil(t, err)
			assert.Equal(t, pair[1], value)
		}
	})

}

func TestDb_Segments(t *testing.T) {
	dbDir := filepath.Join(os.TempDir(), "test-db")
	limit := 23 * 3 * Byte
	db, err := NewDb(dbDir, limit)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	defer os.RemoveAll(dbDir)

	pairs := [][]string{
		{"key1", "value1"}, //13 + 4 (key1) + 6 (value1) -> 23
		{"key2", "value2"},
		{"key3", "value3"},
	}

	newPairs := [][]string{
		{"key1", "value1new"}, // 13 + 4 (key1) + 9 (value1new) -> 26
		{"key2", "new"},       // Needs to be 20 in order to fit in the segment after merge (69 - (23 + 26) = 20)
		{"key4", "value4new"},
	}

	t.Run("segmentation", func(t *testing.T) {
		for _, pair := range pairs {
			err := db.PutString(pair[0], pair[1])
			assert.Nil(t, err, "Cannot put %s: %s", pair, err)
		}

		assert.Equal(t, 1, len(db.segments), "Expected number of segments %d got %d", 1, len(db.segments))
	})

	t.Run("new segment", func(t *testing.T) {
		for _, pair := range newPairs {
			err := db.PutString(pair[0], pair[1])
			assert.Nil(t, err, "Cannot put %s: %s", pair, err)
		}
		assert.Equal(t, 3, len(db.segments), "Expected number of segments %d got %d", 3, len(db.segments))

		for _, pair := range newPairs {
			value, err := db.GetString(pair[0])
			assert.Nil(t, err, "Cannot get %s: %s", pair, err)
			assert.Equal(t, pair[1], value, "Bad value returned expected %s, got %s", pair[1], value)
		}

		val, err := db.GetString("key3")
		assert.Nil(t, err, "Cannot get key3: %s", err)
		assert.Equal(t, "value3", val, "Bad value returned expected value3, got %s", val)
	})

	t.Run("new db process", func(t *testing.T) {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db, err = NewDb(dbDir, limit)
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, 3, len(db.segments), "Expected number of segments %d got %d", 3, len(db.segments))

		for _, pair := range newPairs {
			value, err := db.GetString(pair[0])
			assert.Nil(t, err, "Cannot get %s: %s", pair, err)
			assert.Equal(t, pair[1], value, "Bad value returned expected %s, got %s", pair[1], value)
		}
	})

	t.Run("merge segments", func(t *testing.T) {
		err := db.mergeOldSegments()
		assert.Nil(t, err, "Cannot merge segments: %s", err)
		assert.Equal(t, 2, len(db.segments), "Expected number of segments %d got %d", 2, len(db.segments))
	})

	t.Run("new db process after merge", func(t *testing.T) {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db, err = NewDb(dbDir, limit)
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, 2, len(db.segments), "Expected number of segments %d got %d", 2, len(db.segments))
		assert.Equal(t, 3, db.lastSegmentId)

		for _, pair := range newPairs {
			value, err := db.GetString(pair[0])
			assert.Nil(t, err, "Cannot get %s: %s", pair, err)
			assert.Equal(t, pair[1], value, "Bad value returned expected %s, got %s", pair[1], value)
		}

		val, err := db.GetString("key3")
		assert.Nil(t, err, "Cannot get key3: %s", err)
		assert.Equal(t, "value3", val, "Bad value returned expected value3, got %s", val)
	})

	t.Run("mix of vals", func(t *testing.T) {
		err := db.PutInt64("key5", 123)
		assert.Nil(t, err)
		err = db.PutString("key6", "123")
		assert.Nil(t, err)

		intVal, err := db.GetInt64("key5")
		assert.Nil(t, err)

		strVal, err := db.GetString("key6")
		assert.Nil(t, err)

		_, err = db.GetString("key5")
		assert.Error(t, err)
		_, err = db.GetInt64("key6")
		assert.Error(t, err)

		assert.Equal(t, int64(123), intVal)
		assert.Equal(t, "123", strVal)
	})

	t.Run("new db process with mix of vals", func(t *testing.T) {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db, err = NewDb(dbDir, limit)
		if err != nil {
			t.Fatal(err)
		}

		intVal, err := db.GetInt64("key5")
		assert.Nil(t, err)
		strVal, err := db.GetString("key6")
		assert.Nil(t, err)
		assert.Equal(t, int64(123), intVal)
		assert.Equal(t, "123", strVal)
	})

	t.Run("over limit", func(t *testing.T) {
		err := db.PutString("key5", string(make([]byte, limit+1)))
		assert.Error(t, err, "Expected error, got nil")
	})
}
