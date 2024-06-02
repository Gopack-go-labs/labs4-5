package datastore

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

var ErrNotFound = fmt.Errorf("record does not exist")

type Db struct {
	maxSegmentSize MemoryUnit
	outDir         string

	segments              []*Segment
	curSegment            *Segment
	segmentMergeThreshold int

	dataChan chan PutRequest
	open     bool
}

type PutRequest struct {
	entry *entry
	res   chan error
}

func NewDb(dir string, size MemoryUnit) (*Db, error) {
	err := os.MkdirAll(dir, 0o755)
	if err != nil {
		return nil, err
	}
	db := &Db{
		outDir:                dir,
		segments:              make([]*Segment, 0),
		maxSegmentSize:        size,
		dataChan:              make(chan PutRequest),
		open:                  true,
		segmentMergeThreshold: 10,
	}

	go db.handleWriteLoop()

	return db.recover()
}

const bufSize = 8192

func (db *Db) recover() (*Db, error) {
	files, err := filepath.Glob(filepath.Join(db.outDir, "segment-*"))
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
		seg, err := db.recoverSegment(i, file)
		if err != nil && err != io.EOF {
			return nil, err
		}

		db.segments = append(db.segments, seg)
		if isLastSegment := i == len(files)-1; isLastSegment {
			db.curSegment = seg
		} else {
			err := db.segments[i].Close()
			if err != nil {
				return nil, err
			}
		}
	}

	return db, nil
}

func (db *Db) recoverSegment(id int, path string) (*Segment, error) {
	input, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	segment := &Segment{
		offset: 0,
		file:   input,
		index:  ConcurrentMapInit[string, int64](),
		id:     id,
	}

	for pair := range segmentValsGenerator(segment) {
		if pair.err != nil {
			return nil, pair.err
		}
		e := pair.entry
		segment.index.Set(e.key, segment.offset)
		segment.offset += e.Size().Bytes()
	}

	return segment, nil
}

func (db *Db) Close() error {
	db.open = false
	return db.curSegment.Close()
}

func (db *Db) Get(key string) (val string, err error) {
	for i := len(db.segments) - 1; i >= 0; i-- {
		seg := db.segments[i]
		val, err := seg.Get(key)
		if err != nil {
			continue
		}
		return val, err
	}

	return "", ErrNotFound
}

func (db *Db) Put(key, value string) error {
	res := make(chan error)
	db.dataChan <- PutRequest{
		entry: &entry{
			key:   key,
			value: value,
		},
		res: res,
	}
	err := <-res
	close(res)
	return err
}

func (db *Db) put(e *entry) error {
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

	err := db.curSegment.Write(e)
	return err
}

func (db *Db) mergeOldSegments() error {
	//segments := make([]*Segment, 0, len(db.segments))
	//for _, segment := range db.segments {
	//	if segment != db.curSegment {
	//		segments = append(segments, segment)
	//	}
	//}
	//sort.Slice(segments, func(i, j int) bool {
	//	return segments[i].id < segments[j].id
	//})
	//
	//var err error
	//vals := make(map[string]string)
	//for _, seg := range segments {
	//	for pair := range segmentValsGenerator(seg) {
	//		if pair.err != nil {
	//			return pair.err
	//		}
	//
	//		record, _ := db.index.Get(pair.entry.key)
	//		if inNewerSegment := record.segment.id == db.curSegment.id; inNewerSegment {
	//			continue
	//		}
	//
	//		e := pair.entry
	//		vals[e.key] = e.value
	//	}
	//}
	//
	//shadowDb, err := NewDb(path.Join(db.outDir, "shadow"), db.maxSegmentSize)
	//if err != nil {
	//	return err
	//}
	//defer os.RemoveAll(shadowDb.outDir)
	//
	//for key, value := range vals {
	//	err = shadowDb.Put(key, value)
	//	if err != nil {
	//		return err
	//	}
	//}
	//
	//for key := range vals {
	//	db.index.ReplaceOwn(key, shadowDb.index)
	//}
	//
	//for _, segment := range segments {
	//	toRemove := db.segments[segment.id]
	//	os.Remove(toRemove.file.Name())
	//	//delete(db.segments, segment.id)
	//}
	//
	//for _, segment := range shadowDb.segments {
	//	err = os.Rename(segment.file.Name(), path.Join(db.outDir, path.Base(segment.file.Name())))
	//	if err != nil {
	//		panic("Fatal error during db merge, data loss possible: " + err.Error())
	//	}
	//	db.segments[segment.id] = segment
	//}
	//
	return nil
}

func (db *Db) initNewSegment() error {
	oldId := -1
	if db.curSegment != nil {
		oldId = db.curSegment.id
		defer db.curSegment.Close()
	}

	newSegmentId := oldId + 1
	outFile, err := os.OpenFile(filepath.Join(db.outDir, fmt.Sprintf("segment-%d", newSegmentId)), os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return err
	}

	newSegment := &Segment{
		offset: 0,
		file:   outFile,
		index:  ConcurrentMapInit[string, int64](),
		id:     newSegmentId,
	}
	db.segments = append(db.segments, newSegment)
	db.curSegment = newSegment

	if len(db.segments) > db.segmentMergeThreshold {
		db.mergeOldSegments()
	}

	return nil
}

func (db *Db) handleWriteLoop() {
	for db.open {
		data := <-db.dataChan
		err := db.put(data.entry)
		data.res <- err
	}
}
