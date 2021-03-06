package fs

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
)

// If the key (string) has suffix '/', it denotes a directory;
// otherwise it is a file.
type InMemFS map[string]*bytes.Buffer

var (
	DefaultInMemFS InMemFS = make(InMemFS)
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
func (im InMemFS) Create(name string) io.WriteCloser {
	if _, ok := im[name]; ok {
		im[name] = nil
	}
	im[name] = new(bytes.Buffer)
	return NopCloser(im[name])
}

func (im InMemFS) Open(name string) (io.ReadCloser, error) {
	if r, ok := im[name]; ok {
		return ioutil.NopCloser(r), nil
	}
	return nil, errors.New("File does not exists")
}

func (im InMemFS) ReadDir(name string) ([]os.FileInfo, error) {
	if name[len(name)-1] != '/' {
		name += "/" // Make sure name is a directory.
	}

	if _, ok := im[name]; !ok {
		return nil, &os.PathError{
			Op:   "ReadDir",
			Path: name,
			Err:  os.ErrNotExist}
	}

	r := make([]os.FileInfo, 0)
	for k, v := range im {
		if strings.HasPrefix(k, name) && k != name { // Don't count the directory itself.
			n := path.Base(strings.TrimPrefix(k, name))
			r = append(r, &FileInfo{
				name: n,
				size: int64(v.Len()),
				mode: os.FileMode(0777),
				time: 0, // InMemFS has no real timestamp
				dir:  n[len(n)-1] == '/'})
		}
	}
	return r, nil
}

func (im InMemFS) Exists(name string) bool {
	_, ok := im[name]
	return ok
}

func (im InMemFS) MkDir(name string) {
	if name[len(name)-1] != '/' {
		name = name + "/"
	}
	im[name] = new(bytes.Buffer)
}

func (im InMemFS) Stat(name string) (os.FileInfo, error) {
	if _, ok := im[name]; ok {
		return &FileInfo{
			name: path.Base(name),
			size: int64(im[name].Len()),
			mode: os.FileMode(0777),
			time: 0,
			dir:  name[len(name)-1] == '/'}, nil
	}
	return nil, &os.PathError{
		Op:   "Stat",
		Path: name,
		Err:  os.ErrNotExist}
}
