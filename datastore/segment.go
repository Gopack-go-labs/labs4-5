package datastore

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
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

type generatorPair struct {
	entry *entry
	err   error
}

/**
 * segmentValsGenerator returns a channel that generates entries from a segment file.
 * It is similar to the iterator pattern, but implemented with goroutines.
 */
func segmentValsGenerator(seg *Segment) <-chan *generatorPair {
	ch := make(chan *generatorPair)

	go func() {
		offset := 0
		var buf [bufSize]byte
		reopen, err := os.Open(seg.file.Name())
		if err != nil {
			ch <- &generatorPair{err: err}
			close(ch)
			return
		}
		in := bufio.NewReaderSize(reopen, bufSize)

		for err == nil {
			var (
				header, data []byte
				n            int
			)
			header, err = in.Peek(bufSize)
			if err == io.EOF {
				if len(header) == 0 {
					close(ch)
					return
				}
			} else if err != nil {
				ch <- &generatorPair{err: err}
				close(ch)
				return
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
					ch <- &generatorPair{err: fmt.Errorf("corrupted file")}
					close(ch)
					return
				}

				var e entry
				e.Decode(data)

				ch <- &generatorPair{entry: &e}
				offset += n
			}
		}
	}()
	return ch
}
