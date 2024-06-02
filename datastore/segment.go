package datastore

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
)

type Segment struct {
	offset int64
	file   *os.File
	index  map[string]int64
	mu     sync.RWMutex
	id     int
}

func (s *Segment) Close() error {
	return s.file.Close()
}

func (s *Segment) Write(p *entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	n, err := s.file.Write(p.Encode())
	if err != nil {
		return err
	}
	pos := s.offset
	s.offset += int64(n)

	s.SetIndex(p.key, pos)

	return nil
}

func (s *Segment) Get(key string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	file, err := os.Open(s.file.Name())
	if err != nil {
		return "", err
	}
	defer file.Close()

	pos, ok := s.GetIndex(key)
	if !ok {
		return "", fmt.Errorf("can not get an element")
	}

	_, err = file.Seek(pos, 0)
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

func (s *Segment) Has(key string) bool {
	_, err := s.Get(key)
	return err == nil
}

func (s *Segment) IsSurpassed(maxSize MemoryUnit) bool {
	return s.offset > maxSize.Bytes()
}

func (s *Segment) FilePath() string {
	return s.file.Name()
}

func (s *Segment) GetIndex(key string) (int64, bool) {
	offset, ok := s.index[key]
	return offset, ok
}

func (s *Segment) SetIndex(key string, val int64) {
	s.index[key] = val
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
