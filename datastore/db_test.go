package datastore

import (
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

	outFile := db.curSegment.file
	if err != nil {
		t.Fatal(err)
	}

	t.Run("put/get", func(t *testing.T) {
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
			value, err := db.Get(pair[0])
			if err != nil {
				t.Errorf("Cannot get %s: %s", pairs[0], err)
			}
			if value != pair[1] {
				t.Errorf("Bad value returned expected %s, got %s", pair[1], value)
			}
		}
	})

	outInfo, err := outFile.Stat()
	if err != nil {
		t.Fatal(err)
	}
	size1 := outInfo.Size()

	t.Run("file growth", func(t *testing.T) {
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
		}
		outInfo, err := outFile.Stat()
		if err != nil {
			t.Fatal(err)
		}
		if size1*2 != outInfo.Size() {
			t.Errorf("Unexpected size (%d vs %d)", size1, outInfo.Size())
		}
	})

	t.Run("new db process", func(t *testing.T) {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db, err = NewDb(dir, 10*Megabyte)
		if err != nil {
			t.Fatal(err)
		}

		for _, pair := range pairs {
			value, err := db.Get(pair[0])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
			if value != pair[1] {
				t.Errorf("Bad value returned expected %s, got %s", pair[1], value)
			}
		}
	})

}

func TestDb_Segments(t *testing.T) {
	dbDir := filepath.Join(os.TempDir(), "test-db")
	if err := os.MkdirAll(dbDir, 0o700); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dbDir)

	db, err := NewDb(dbDir, 18*3*Byte)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	pairs := [][]string{
		{"key1", "value1"}, //12 + 4 (key1) + 6 (value1) -> 22
		{"key2", "value2"},
		{"key3", "value3"},
	}

	t.Run("segmentation", func(t *testing.T) {
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
		}
		if len(db.segments) != 1 {
			t.Errorf("Expected number of segments %d got %d", 1, len(db.segments))
		}
	})

	t.Run("new segment", func(t *testing.T) {
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
		}
		if len(db.segments) != 2 {
			t.Errorf("Expected number of segments %d got %d", 2, len(db.segments))
		}
	})

	t.Run("new db process", func(t *testing.T) {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db, err = NewDb(dbDir, 18*3*Byte)
		if err != nil {
			t.Fatal(err)
		}

		if len(db.segments) != 2 {
			t.Errorf("Expected number of segments %d got %d", 2, len(db.segments))
		}
	})
}
