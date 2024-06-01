package datastore

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
)

var ErrNotFound = fmt.Errorf("record does not exist")

type hashIndex map[string]*Record

type Db struct {
	maxSegmentSize MemoryUnit
	outDir         string

	index      hashIndex
	segments   map[int]*Segment
	curSegment *Segment
}

func NewDb(dir string, size MemoryUnit) (*Db, error) {
	err := os.MkdirAll(dir, 0o755)
	if err != nil {
		return nil, err
	}
	db := &Db{
		outDir:         dir,
		index:          make(hashIndex),
		segments:       make(map[int]*Segment),
		maxSegmentSize: size,
	}
	files, err := filepath.Glob(filepath.Join(dir, "segment-*"))
	if len(files) == 0 {
		err = db.initNewSegment()
		if err != nil {
			return nil, err
		}
		return db, nil
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i] < files[j]
	})
	for i, file := range files {
		err = db.recoverSegment(i, file)
		db.curSegment = db.segments[i]
		if err != nil && err != io.EOF {
			return nil, err
		}

		if isLastSegment := i == len(files)-1; !isLastSegment {
			err := db.segments[i].Close()
			if err != nil {
				return nil, err
			}
		}
	}
	return db, nil
}

const bufSize = 8192

func (db *Db) recoverSegment(id int, path string) error {
	input, err := os.Open(path)
	if err != nil {
		return err
	}
	segment := &Segment{
		offset: 0,
		file:   input,
		id:     id,
	}
	db.segments[id] = segment

	var buf [bufSize]byte
	in := bufio.NewReaderSize(input, bufSize)
	for err == nil {
		var (
			header, data []byte
			n            int
		)
		header, err = in.Peek(bufSize)
		if err == io.EOF {
			if len(header) == 0 {
				return err
			}
		} else if err != nil {
			return err
		}
		size := binary.LittleEndian.Uint32(header)

		if size < bufSize {
			data = buf[:size]
		} else {
			data = make([]byte, size)
		}
		n, err = in.Read(data)

		if err == nil {
			if n != int(size) {
				return fmt.Errorf("corrupted file")
			}

			var e entry
			e.Decode(data)
			db.index[e.key] = &Record{
				position: segment.offset,
				segment:  segment,
			}
			segment.offset += int64(n)
		}
	}
	return err
}

func (db *Db) Close() error {
	return db.curSegment.Close()
}

func (db *Db) Get(key string) (val string, err error) {
	record, ok := db.index[key]
	if !ok {
		return "", ErrNotFound
	}
	val, err = record.Get()

	return
}

func (db *Db) Put(key, value string) error {
	e := &entry{
		key:   key,
		value: value,
	}
	entrySize := e.Size()
	if db.maxSegmentSize < entrySize {
		return fmt.Errorf("entry size exceeds segment size")
	}
	if db.curSegment.IsSurpassed(db.maxSegmentSize - entrySize) {
		err := db.initNewSegment()
		if err != nil {
			return err
		}
	}

	record, err := db.curSegment.Write(e)

	if err == nil {
		db.index[key] = record
	}
	return err
}

func (db *Db) mergeOldSegments() error {
	segments := make([]*Segment, 0, len(db.segments))
	for _, segment := range db.segments {
		if segment != db.curSegment {
			segments = append(segments, segment)
		}
	}
	sort.Slice(segments, func(i, j int) bool {
		return segments[i].id < segments[j].id
	})

	var err error
	vals := make(map[string]string)
	var buf [bufSize]byte
	for _, seg := range segments {
		offset := 0
		reopen, err := os.Open(seg.file.Name())
		if err != nil {
			return err
		}
		in := bufio.NewReaderSize(reopen, bufSize)

		// TODO: make file iterator
		for err == nil {
			var (
				header, data []byte
				n            int
			)
			header, err = in.Peek(bufSize)
			if err == io.EOF {
				if len(header) == 0 {
					break
				}
			} else if err != nil {
				return err
			}
			size := binary.LittleEndian.Uint32(header)

			if size < bufSize {
				data = buf[:size]
			} else {
				data = make([]byte, size)
			}
			n, err = in.Read(data)

			if err == nil {
				if n != int(size) {
					return fmt.Errorf("corrupted file")
				}

				var e entry
				e.Decode(data)

				vals[e.key] = e.value
				offset += n
			}
		}
	}

	shadowDb, err := NewDb(path.Join(db.outDir, "shadow"), db.maxSegmentSize)
	if err != nil {
		return err
	}
	defer os.RemoveAll(shadowDb.outDir)

	for key, value := range vals {
		err = shadowDb.Put(key, value)
		if err != nil {
			return err
		}
	}

	for key := range vals {
		record := db.index[key]
		if wasUpdatedAfterMerge := record.segment.id > segments[len(segments)-1].id; !wasUpdatedAfterMerge {
			db.index[key] = shadowDb.index[key]
		}
	}

	for _, segment := range segments {
		toRemove := db.segments[segment.id]
		err := os.Remove(toRemove.file.Name())
		if err != nil {
			panic("Fatal error during db merge, data loss possible: " + err.Error())
		}
		delete(db.segments, segment.id)
	}

	for _, segment := range shadowDb.segments {
		err = os.Rename(segment.file.Name(), path.Join(db.outDir, path.Base(segment.file.Name())))
		if err != nil {
			panic("Fatal error during db merge: " + err.Error())
		}
		db.segments[segment.id] = segment
	}

	return nil
}

func (db *Db) initNewSegment() error {
	oldId := -1
	if db.curSegment != nil {
		oldId = db.curSegment.id
	}

	newSegmentId := oldId + 1
	outFile, err := os.OpenFile(filepath.Join(db.outDir, fmt.Sprintf("segment-%d", newSegmentId)), os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return err
	}
	db.segments[newSegmentId] = &Segment{
		offset: 0,
		file:   outFile,
		id:     newSegmentId,
	}

	if db.curSegment != nil {
		err = db.curSegment.Close()
		if err != nil {
			return err
		}
	}

	db.curSegment = db.segments[newSegmentId]
	return nil
}
