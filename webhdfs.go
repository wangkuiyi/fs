// +build webhdfs

package fs

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/user"
	"path"
	"strings"

	"github.com/vladimirvivien/gowfs"
	"github.com/wangkuiyi/file/inmemfs"
)

var (
	hdfs *gowfs.FileSystem
)

func HookupHDFS(addr, role string) error {
	if len(role) <= 0 {
		if u, e := user.Current(); e != nil {
			return fmt.Errorf("Unknown current user: %v", e)
		} else {
			role = u.Username
		}
	}

	log.Printf("Connecting to HDFS %s@%s", role, addr)
	fs, e := gowfs.NewFileSystem(gowfs.Configuration{Addr: addr, User: role})
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
	switch fs, path := FsPath(name); fs {
	case HDFS:
		if !hookedUp() {
			return nil, fmt.Errorf("Not yet hooked up with HDFS before creating %v", name)
		}
		go func() {
			_, e := hdfs.Create(r,
				gowfs.Path{Name: path},
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
	case InMem:
		f := inmemfs.Create(path)
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
	switch fs, path := FsPath(name); fs {
	case HDFS:
		if !hookedUp() {
			return nil, fmt.Errorf("Not yet hooked up with HDFS before opening %v", name)
		}
		r, e := hdfs.Open(gowfs.Path{Name: path}, 0, 0, 0) // default offset, lenght and buffersize
		if e != nil {
			return nil, fmt.Errorf("Cannot open HDFS file %v", name)
		}
		return r, nil
	case InMem:
		r, e := inmemfs.Open(path)
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
	switch fs, path := FsPath(name); fs {
	case HDFS:
		if !hookedUp() {
			return nil, fmt.Errorf("Not yet hooked up with HDFS before listing %v", name)
		}
		is, e := hdfs.ListStatus(gowfs.Path{Name: path})
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
	case InMem:
		is := inmemfs.List(path)
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
	switch fs, path := FsPath(name); fs {
	case HDFS:
		if !hookedUp() {
			return false, fmt.Errorf("Not yet hooked up with HDFS before checking existence of %v", name)
		}
		fs := gowfs.FsShell{FileSystem: hdfs, WorkingPath: "/"}
		// TODO(wyi): confirm that fs.Exists returns false when error.
		return fs.Exists(path)
	case InMem:
		return inmemfs.Exists(path), nil
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

	switch fs, path := FsPath(name); fs {
	case HDFS:
		if !hookedUp() {
			return fmt.Errorf("Not yet hooked up with HDFS before mkdir %v", name)
		}
		_, e := hdfs.MkDirs(gowfs.Path{Name: path}, 0777)
		return e
	case InMem:
		inmemfs.MkDir(path)
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

	if fs, src := FsPath(localFile); fs != Local {
		return false, fmt.Errorf("localFile %s must be local", localFile)
	} else if fs, dest := FsPath(hdfsPath); fs != HDFS {
		return false, fmt.Errorf("hdfsPath %s has no HDFSPrefix", hdfsPath)
	} else {
		fs := &gowfs.FsShell{FileSystem: hdfs, WorkingPath: "/"}
		return fs.Put(src, dest, true)
	}
}

// TODO(wyi): Add unit test for Stat.
func Stat(name string) (Info, error) {
	switch fs, p := FsPath(name); fs {
	case HDFS:
		if !hookedUp() {
			return Info{}, fmt.Errorf("Not yet hooked up with HDFS before stat %v", name)
		}
		fs, e := hdfs.GetFileStatus(gowfs.Path{Name: p})
		if e != nil {
			return Info{}, fmt.Errorf("hdfs.GetFileStatus(%s): %v", name, e)
		} else {
			return Info{path.Base(name), fs.Length, fs.Type == "DIRECTORY"}, nil
		}
	case InMem:
		fi := inmemfs.Stat(p)
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
