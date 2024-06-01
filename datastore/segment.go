package datastore

import (
	"bufio"
	"os"
)

type Record struct {
	position int64
	segment  *Segment
}

func (r *Record) Get() (string, error) {
	return r.segment.Get(r)
}

type Segment struct {
	offset int64
	file   *os.File
	id     int
}

func (s *Segment) Close() error {
	return s.file.Close()
}

func (s *Segment) Write(p *entry) (*Record, error) {
	n, err := s.file.Write(p.Encode())
	if err != nil {
		return nil, err
	}
	record := &Record{
		segment:  s,
		position: s.offset,
	}
	s.offset += int64(n)
	return record, nil
}

func (s *Segment) Get(record *Record) (string, error) {
	file, err := os.Open(s.file.Name())
	if err != nil {
		return "", err
	}
	defer file.Close()

	_, err = file.Seek(record.position, 0)
	if err != nil {
		return "", err
	}

	reader := bufio.NewReader(file)
	value, err := readValue(reader)
	if err != nil {
		return "", err
	}

	return value, nil
}

func (s *Segment) IsSurpassed(maxSize MemoryUnit) bool {
	return s.offset > maxSize.Bytes()
}

func (s *Segment) FilePath() string {
	return s.file.Name()
}
