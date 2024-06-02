package datastore

import (
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
)

var ErrNotFound = fmt.Errorf("record does not exist")

type Db struct {
	maxSegmentSize MemoryUnit
	outDir         string

	segments              []*Segment
	lastSegmentId         int
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

func (db *Db) curSegment() *Segment {
	if len(db.segments) == 0 {
		return nil
	}
	return db.segments[len(db.segments)-1]
}

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
		seg, err := db.recoverSegment(file)
		if err != nil && err != io.EOF {
			return nil, err
		}

		db.segments = append(db.segments, seg)
		db.lastSegmentId = seg.id

		if isLastSegment := i == len(files)-1; !isLastSegment {
			err := db.segments[i].Close()
			if err != nil {
				return nil, err
			}
		}
	}

	return db, nil
}

func (db *Db) recoverSegment(path string) (*Segment, error) {
	id, err := db.getSegmentId(path)
	if err != nil {
		return nil, err
	}

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
	return db.curSegment().Close()
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
	if db.curSegment().IsSurpassed(db.maxSegmentSize - entrySize) {
		err := db.initNewSegment()
		if err != nil {
			return err
		}
	}

	err := db.curSegment().Write(e)
	return err
}

func (db *Db) mergeOldSegments() error {
	segmentsToMerge := make([]*Segment, 0, len(db.segments))
	for i := 0; i < len(db.segments)-1; i++ {
		seg := db.segments[i]
		segmentsToMerge = append(segmentsToMerge, seg)
	}

	var err error
	vals := make(map[string]string)
	for i := 0; i < len(segmentsToMerge); i++ {
		seg := segmentsToMerge[i]
		for pair := range segmentValsGenerator(seg) {
			if pair.err != nil {
				return pair.err
			}

			if db.curSegment().Has(pair.entry.key) {
				continue
			}

			e := pair.entry
			vals[e.key] = e.value
		}
	}

	shadowDb, err := NewDb(path.Join(db.outDir, "shadow"), db.maxSegmentSize)
	if err != nil {
		return err
	}
	defer os.RemoveAll(shadowDb.outDir)
	defer shadowDb.Close()

	for key, value := range vals {
		err = shadowDb.Put(key, value)
		if err != nil {
			return err
		}
	}

	for _, mergedSegment := range shadowDb.segments {
		err = os.Rename(mergedSegment.file.Name(), db.getNextSegmentPath())
		if err != nil {
			return err
		}
		db.lastSegmentId++
	}

	db.segments = append(shadowDb.segments, db.curSegment())

	for _, segment := range segmentsToMerge {
		os.Remove(segment.FilePath())
	}

	return nil
}

func (db *Db) initNewSegment() error {
	oldId := -1
	if db.curSegment() != nil {
		oldId = db.curSegment().id
		defer db.curSegment().Close()
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

	if len(db.segments) > db.segmentMergeThreshold {
		go db.mergeOldSegments()
	}

	return nil
}

func (db *Db) getNextSegmentPath() string {
	id := db.lastSegmentId + 1
	return filepath.Join(db.outDir, fmt.Sprintf("segment-%d", id))
}

func (db *Db) handleWriteLoop() {
	for db.open {
		data := <-db.dataChan
		err := db.put(data.entry)
		data.res <- err
	}
}

func (db *Db) getSegmentId(path string) (int, error) {
	s := regexp.MustCompile(`segment-(\d+)`).FindStringSubmatch(path)
	if len(s) == 0 {
		return 0, fmt.Errorf("cannot parse segment id")
	}

	return strconv.Atoi(s[1])
}
