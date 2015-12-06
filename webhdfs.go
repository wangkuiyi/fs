// +build webhdfs

package fs

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"

	"github.com/vladimirvivien/gowfs"
	"github.com/wangkuiyi/file/inmemfs"
)

var (
	hdfs *gowfs.FileSystem
)

func HookupHDFS(addr, user string) error {
	if len(user) <= 0 {
		user = os.Getenv("USER")
		if len(user) <= 0 {
			return errors.New("Specify HDFS user specificially or via $USER")
		}
	}
	log.Printf("Connecting to HDFS %s@%s", user, addr)
	fs, e := gowfs.NewFileSystem(gowfs.Configuration{Addr: addr, User: user})
	if e != nil {
		return e
	}
	hdfs = fs
	return testConnection()
}

func hookedUp() bool {
	return hdfs != nil
}

func testConnection() error {
	_, e := hdfs.ListStatus(gowfs.Path{Name: "/"})
	if e != nil {
		return fmt.Errorf("Unable to connect to server: %v", e)
	}
	log.Printf("Connected to %s. OK.\n", hdfs.Config.Addr)
	return nil
}

// Create returns the writer end of a Go pipe and starts a goroutine
// that copies from the reader end of the pipe to either a local file
// or an HDFS file.  If Create returns without error, the caller is
// expected to write into the returned writer end.  After writing, the
// caller must close the writer end to acknowledge the EOF.
func Create(name string) (io.WriteCloser, error) {
	r, w := io.Pipe()
	switch {
	case strings.HasPrefix(name, HDFSPrefix):
		if !hookedUp() {
			return nil, fmt.Errorf("Not yet hooked up with HDFS before creating %v", name)
		}
		go func() {
			_, e := hdfs.Create(r,
				gowfs.Path{Name: strings.TrimPrefix(name, HDFSPrefix)},
				true, // overwrite
				0, 0, // default blocksize and replica
				0700, // only the owner can access
				0)    // default buffer size
			if e != nil {
				r.Close()
				w.Close()
				log.Panicf("Failed piping to file %s: %v", name, e)
			}
		}()
	case strings.HasPrefix(name, InMemPrefix):
		f := inmemfs.Create(strings.TrimPrefix(name, InMemPrefix))
		go func() {
			defer r.Close()
			_, e := io.Copy(f, r)
			if e != nil {
				log.Panicf("Failed piping to file %s: %v", name, e)
			}
		}()
	default:
		f, e := os.Create(name)
		if e != nil {
			r.Close()
			w.Close()
			return nil, errors.New("Cannot create file.")
		}
		go func() {
			defer f.Close()
			defer r.Close()
			_, e := io.Copy(f, r)
			if e != nil {
				log.Panicf("Failed piping to file %s: %v", name, e)
			}
		}()
	}
	return w, nil
}

func Open(name string) (io.ReadCloser, error) {
	switch {
	case strings.HasPrefix(name, HDFSPrefix):
		if !hookedUp() {
			return nil, fmt.Errorf("Not yet hooked up with HDFS before opening %v", name)
		}
		r, e := hdfs.Open(gowfs.Path{Name: strings.TrimPrefix(name, HDFSPrefix)}, 0, 0, 0) // default offset, lenght and buffersize
		if e != nil {
			return nil, fmt.Errorf("Cannot open HDFS file %v", name)
		}
		return r, nil
	case strings.HasPrefix(name, InMemPrefix):
		r, e := inmemfs.Open(strings.TrimPrefix(name, InMemPrefix))
		if e != nil {
			return nil, fmt.Errorf("Cannot open in-memory file %v", name)
		}
		return r, nil
	default:
		f, e := os.Open(name)
		if e != nil {
			return nil, fmt.Errorf("Cannot open local file %v", name)
		}
		return f, nil

	}
}

type Info struct {
	Name  string
	Size  int64
	IsDir bool
}

func List(name string) ([]Info, error) {
	switch {
	case strings.HasPrefix(name, HDFSPrefix):
		if !hookedUp() {
			return nil, fmt.Errorf("Not yet hooked up with HDFS before listing %v", name)
		}
		is, e := hdfs.ListStatus(gowfs.Path{Name: strings.TrimPrefix(name, HDFSPrefix)})
		if e != nil {
			return nil, e
		}
		if len(is) > 0 {
			ss := make([]Info, len(is))
			for i, s := range is {
				ss[i].Name = s.PathSuffix
				ss[i].Size = s.Length
				ss[i].IsDir = (s.Type == "DIRECTORY")
			}
			return ss, nil
		}
		return nil, nil
	case strings.HasPrefix(name, InMemPrefix):
		is := inmemfs.List(strings.TrimPrefix(name, InMemPrefix))
		if len(is) > 0 {
			ss := make([]Info, len(is))
			for i, s := range is {
				ss[i].Name = s.Name
				ss[i].Size = s.Size
				ss[i].IsDir = s.IsDir
			}
			return ss, nil
		}
		return nil, nil
	default:
		is, e := ioutil.ReadDir(name)
		if e != nil {
			return nil, e
		}
		if len(is) > 0 {
			ss := make([]Info, len(is))
			for i, s := range is {
				ss[i].Name = s.Name()
				ss[i].Size = s.Size()
				ss[i].IsDir = s.IsDir()
			}
			return ss, nil
		}
		return nil, nil
	}
}

// Exists returns false, if there is any error.
func Exists(name string) (bool, error) {
	switch {
	case strings.HasPrefix(name, HDFSPrefix):
		if !hookedUp() {
			return false, fmt.Errorf("Not yet hooked up with HDFS before checking existence of %v", name)
		}
		fs := gowfs.FsShell{hdfs, "/"}
		// TODO(wyi): confirm that fs.Exists returns false when error.
		return fs.Exists(strings.TrimPrefix(name, HDFSPrefix))
	case strings.HasPrefix(name, InMemPrefix):
		return inmemfs.Exists(strings.TrimPrefix(name, InMemPrefix)), nil
	default:
		_, e := os.Stat(name)
		if e != nil {
			if os.IsNotExist(e) {
				return false, nil
			} else {
				return false, fmt.Errorf("Exists(%s): %v", name, e)
			}
		}
		return true, nil
	}
}

// Create a directory, along with any necessary parents.  If the
// directory is already there, it returns nil.
//
// TODO(wyi): Add unit test for this function.
func MkDir(name string) error {
	if len(strings.Join(strings.Split(name, ":")[1:], "")) == 0 {
		// As path.Dir("file:/a") returns "file:" instead of "file:/".
		return nil
	}

	switch {
	case strings.HasPrefix(name, HDFSPrefix):
		if !hookedUp() {
			return fmt.Errorf("Not yet hooked up with HDFS before mkdir %v", name)
		}
		_, e := hdfs.MkDirs(gowfs.Path{Name: strings.TrimPrefix(name, HDFSPrefix)}, 0777)
		return e
	case strings.HasPrefix(name, InMemPrefix):
		inmemfs.MkDir(strings.TrimPrefix(name, InMemPrefix))
		return nil
	default:
		return os.MkdirAll(name, 0777)
	}
}

// Put copy a local file to HDFS.  It overwrites if the destination
// already exists.
//
// BUG(wyi): hdfsPath must name a directory.  And due to a bug in
// "github.com/vladimirvivien/gowfs", this directory must not be the
// root directory "hdfs:/".
func Put(localFile, hdfsPath string) (bool, error) {
	if !hookedUp() {
		return false, fmt.Errorf("Not yet hooked up with HDFS before put %v to %v", localFile, hdfsPath)
	}

	if strings.HasPrefix(localFile, HDFSPrefix) || strings.HasPrefix(localFile, InMemPrefix) {
		return false, fmt.Errorf("localFile %s must be local", localFile)
	}

	if !strings.HasPrefix(hdfsPath, HDFSPrefix) {
		return false, fmt.Errorf("hdfsPath %s has no HDFSPrefix", hdfsPath)
	}
	hdfsPath = strings.TrimPrefix(hdfsPath, HDFSPrefix)

	fs := &gowfs.FsShell{hdfs, "/"}
	return fs.Put(localFile, hdfsPath, true)
}

// TODO(wyi): Add unit test for Stat.
func Stat(name string) (Info, error) {
	switch {
	case strings.HasPrefix(name, HDFSPrefix):
		if !hookedUp() {
			return Info{}, fmt.Errorf("Not yet hooked up with HDFS before stat %v", name)
		}

		fs, e := hdfs.GetFileStatus(gowfs.Path{Name: strings.TrimPrefix(name, HDFSPrefix)})
		if e != nil {
			return Info{}, fmt.Errorf("hdfs.GetFileStatus(%s): %v", name, e)
		} else {
			return Info{path.Base(name), fs.Length, fs.Type == "DIRECTORY"}, nil
		}
	case strings.HasPrefix(name, InMemPrefix):
		fi := inmemfs.Stat(strings.TrimPrefix(name, InMemPrefix))
		if len(fi.Name) > 0 {
			return Info{fi.Name, fi.Size, fi.IsDir}, nil
		} else {
			return Info{}, fmt.Errorf("inmemfs.Info(%s): File not exist", name)
		}
	default:
		if fi, e := os.Stat(name); e != nil {
			return Info{}, fmt.Errorf("os.Stat(%s): %v", name, e)
		} else {
			return Info{path.Base(name), fi.Size(), fi.IsDir()}, nil
		}
	}
}
