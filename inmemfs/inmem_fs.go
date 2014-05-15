package inmemfs

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"path"
	"strings"
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

type Info struct {
	Name  string
	Size  int64
	IsDir bool
}

func List(name string) []*Info {
	r := make([]*Info, 0)
	for k, v := range inMem {
		if strings.HasPrefix(k, name) {
			n := path.Base(strings.TrimPrefix(k, name))
			r = append(r, &Info{
				Name:  n,
				Size:  int64(v.Len()),
				IsDir: n[len(n)-1] == '/'})
		}
	}
	return r
}

func Format() {
	inMem = make(InMemoryFilesystem)
}

func Exists(name string) bool {
	_, ok := inMem[name]
	return ok
}
