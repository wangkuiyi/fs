package inmemfs

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
)

type InMemoryFilesystem map[string]*bytes.Buffer

var (
	inMem InMemoryFilesystem = make(InMemoryFilesystem)
)

type nopCloser struct {
	io.Writer
}

func (nopCloser) Close() error { return nil }

// NopCloser returns a WriteCloser with a no-op Close method wrapping
// the provided Writer w.
func NopCloser(w io.Writer) io.WriteCloser {
	return nopCloser{w}
}

// Create creates a file with given name.  If that file already
// exists, it is truncated.
func Create(name string) io.WriteCloser {
	if _, ok := inMem[name]; ok {
		inMem[name] = nil
	}
	inMem[name] = new(bytes.Buffer)
	return NopCloser(inMem[name])
}

func Open(name string) (io.ReadCloser, error) {
	if r, ok := inMem[name]; ok {
		return ioutil.NopCloser(r), nil
	}
	return nil, errors.New("File does not exists")
}
