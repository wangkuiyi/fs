// Package file provide uniform interface to access local filesystem,
// Hadoop filesystem (HDFS), and an in-memory filesystem define in
// https://github.com/wangkuiyi/file/inmemfs.
//
// It accesses HDFS either via WebHDFS implemented in
// github.com/vladimirvivien/gowfs", or the protobuf-based native RPC
// implemented in write-support branch of github.com/colinmarc/hdfs.
// The choice between these two options is via Go build tag webhdfs.
// By default it uses the native RPC.
//
// In order to connect to HDFS, you need to manually invoke
// HookupHDFS(), typically in main():
/*
	import (
	  "flag"
	  "github.com/wangkuiyi/fs"
	)

	func main() {
          namenode := flag.String("hdfsAddr", "localhost:9000", "Either namenode addr or WebHDFS addr.")
          hdfsUser := flag.String("hdfsUser", "", "HDFS username. Could be empty.")
	  flag.Parse()
	  fs.HookupHDFS(hdfsAddr, hdfsUser)
	  ...
	}
*/
//
// For more about the usage of this package, you might want to check
// and run the unit tests.  If you do not have HDFS set up for
// development and you do not want to run unit tests that usse HDFS,
// you can set the DISABLE_HDFS_TEST environment variable.
//
// To setup a single-node HDFS for development and testing, we need to
// edit $HADOOP_HOME/etc/hadoop/core-site.xml
/*
	<configuration>
	  <property>
		<name>fs.defaultFS</name>
		<value>hdfs://localhost:9000</value>
		<description>NameNode URI</description>
	  </property>
	  <property>
		<name>hadoop.http.staticuser.user</name>
		<value>true</value>
	  </property>
	</configuration>
*/
// and $HADOOP_HOME/etc/hadoop/hdfs-site.xml:
/*
	<configuration>
	  <property>
		<name>dfs.datanode.data.dir</name>
		<value>file:///Users/yiwang/hadoop/hdfs/datanode</value>
	  </property>
	  <property>
		<name>dfs.namenode.name.dir</name>
		<value>file:///Users/yiwang/hadoop/hdfs/namenode</value>
	  </property>
	  <property>
		<name>dfs.webhdfs.enabled</name>
		<value>true</value>
	  </property>
	  <property>
		<name>dfs.replication</name>
		<value>1</value>
	  </property>
	  <property>
		<name>dfs.client.block.write.replace-datanode-on-failure.enable</name>
		<value>false</value>
	  </property>
	</configuration>
*/
// In above samples, it is assumed that $HADOOP_HOME is at
// /Users/yiwang/hadoop. You would want to adapt the path to fit your
// case.
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
	"strconv"
	"strings"
	"time"

	"github.com/colinmarc/hdfs"
	"github.com/vladimirvivien/gowfs"
)

type Type int

const (
	Local Type = iota
	InMem Type = iota
	WebFS Type = iota
	HDFS  Type = iota
)

func FsPath(path string) (Type, string) {
	switch {
	case strings.HasPrefix(path, "/webfs/"):
		return WebFS, "/" + strings.TrimPrefix(path, "/webfs/")
	case strings.HasPrefix(path, "/hdfs/"):
		return HDFS, "/" + strings.TrimPrefix(path, "/hdfs/")
	case strings.HasPrefix(path, "/inmem/"):
		return InMem, "/" + strings.TrimPrefix(path, "/inmem/")
	default:
		return Local, path
	}
}

// FileInfo implements os.FileInfo
type FileInfo struct {
	name string
	size int64
	mode os.FileMode
	time int64
	dir  bool
}

func (i FileInfo) Name() string {
	return i.name
}

func (i FileInfo) Size() int64 {
	return i.size
}

func (i FileInfo) Mode() os.FileMode {
	return i.mode
}

func (i FileInfo) ModTime() time.Time {
	return time.Unix(i.time, 0)
}

func (i FileInfo) IsDir() bool {
	return i.dir
}

func (i FileInfo) Sys() interface{} {
	return nil
}

var (
	webfs *gowfs.FileSystem
	rpcfs *hdfs.Client
)

func HookupHDFS(namenode, webapi, role string) error {
	err := ""

	if len(role) <= 0 {
		if u, e := user.Current(); e != nil {
			err += fmt.Sprintf("Unknown current user: %v\n", e)
		} else {
			role = u.Username
		}
	}

	log.Printf("Establish HDFS protobuf-based RPC connection as %s@%s", role, namenode)
	if fs, e := hdfs.NewForUser(namenode, role); e != nil {
		err += fmt.Sprintf("Cannot estabilish RPC connection to %s@%s: %v", role, namenode, e)
	} else {
		rpcfs = fs
	}

	log.Printf("Establish WebHDFS connection as %s@%s", role, webapi)
	if fs, e := gowfs.NewFileSystem(gowfs.Configuration{Addr: webapi, User: role}); e != nil {
		err += fmt.Sprintf("Cannot establish WebHDFS connection to %s@%s: %v", role, webapi, e)
	} else {
		webfs = fs
		if e := testConnection(); e != nil {
			err += fmt.Sprintf("Failed checking WebHDFS connection: %v", e)
		}
	}

	if len(err) > 0 {
		return fmt.Errorf(err)
	}
	return nil
}

func testConnection() error {
	_, e := webfs.ListStatus(gowfs.Path{Name: "/"})
	if e != nil {
		return fmt.Errorf("Unable to connect to server: %v", e)
	}
	log.Printf("Connected to %s. OK.\n", webfs.Config.Addr)
	return nil
}

var (
	errNoWebFS = errors.New("Have not established WebHDFS connection")
	errNoRpcFS = errors.New("Have not established protobuf-based RPC connection")
)

// Create returns the writer end of a Go pipe and starts a goroutine
// that copies from the reader end of the pipe to either a local file
// or an HDFS file.  If Create returns without error, the caller is
// expected to write into the returned writer end.  After writing, the
// caller must close the writer end to acknowledge the EOF.
func Create(name string) (io.WriteCloser, error) {
	switch fs, path := FsPath(name); fs {
	case WebFS:
		if webfs == nil {
			return nil, errNoWebFS
		}
		// gowfs.Create requires a reader parameter.
		r, w := io.Pipe()
		go func() {
			_, e := webfs.Create(r,
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
		return w, nil
	case HDFS:
		if rpcfs == nil {
			return nil, errNoRpcFS
		}
		return rpcfs.Create(path)
	case InMem:
		return DefaultInMemFS.Create(path), nil
	default:
		return os.Create(path)
	}
}

func Open(name string) (io.ReadCloser, error) {
	switch fs, path := FsPath(name); fs {
	case WebFS:
		if webfs == nil {
			return nil, errNoWebFS
		}
		return webfs.Open(gowfs.Path{Name: path}, 0, 0, 0) // default offset, lenght and buffersize
	case HDFS:
		if rpcfs == nil {
			return nil, errNoRpcFS
		}
		return rpcfs.Open(path)
	case InMem:
		return DefaultInMemFS.Open(path)
	default:
		return os.Open(path)
	}
}

func ReadDir(name string) ([]os.FileInfo, error) {
	switch fs, path := FsPath(name); fs {
	case WebFS:
		if webfs == nil {
			return nil, errNoWebFS
		}
		is, e := webfs.ListStatus(gowfs.Path{Name: path})
		if e != nil {
			return nil, e
		}
		if len(is) > 0 {
			ss := make([]os.FileInfo, 0, len(is))
			for _, s := range is {
				mode, _ := strconv.ParseUint(s.Permission, 8, 32)
				ss = append(ss, &FileInfo{
					name: s.PathSuffix,
					size: s.Length,
					mode: os.FileMode(mode),
					time: s.ModificationTime,
					dir:  (s.Type == "DIRECTORY"),
				})
			}
			return ss, nil
		}
		return nil, nil
	case HDFS:
		return rpcfs.ReadDir(path)
	case InMem:
		return DefaultInMemFS.ReadDir(path)
	default:
		return ioutil.ReadDir(path)
	}
}

// Create a directory, along with any necessary parents.  If the
// directory is already there, it returns nil.
//
// TODO(wyi): Add unit test for this function.
func Mkdir(name string) error {
	switch fs, path := FsPath(name); fs {
	case WebFS:
		if webfs == nil {
			return errNoWebFS
		}
		_, e := webfs.MkDirs(gowfs.Path{Name: path}, 0777)
		return e
	case HDFS:
		return rpcfs.MkdirAll(path, 0777)
	case InMem:
		DefaultInMemFS.MkDir(path)
		return nil
	default:
		return os.MkdirAll(path, 0777)
	}
}

// Put copy a local file to HDFS.  It overwrites if the destination
// already exists.
//
// BUG(wyi): hdfsPath must name a directory.  And due to a bug in
// "github.com/vladimirvivien/gowfs", this directory must not be the
// root directory "hdfs:/".
func Put(localFile, hdfsPath string) (bool, error) {
	if webfs == nil {
		return false, errNoWebFS
	}

	if fs, src := FsPath(localFile); fs != Local {
		return false, fmt.Errorf("localFile %s must be local", localFile)
	} else if fs, dest := FsPath(hdfsPath); fs != WebFS {
		return false, fmt.Errorf("hdfsPath %s must refer to WebHDFS", hdfsPath)
	} else {
		fs := &gowfs.FsShell{FileSystem: webfs, WorkingPath: "/"}
		return fs.Put(src, dest, true)
	}
}

// TODO(wyi): Add unit test for Stat.
func Stat(name string) (os.FileInfo, error) {
	switch fs, p := FsPath(name); fs {
	case WebFS:
		if webfs == nil {
			return nil, errNoWebFS
		}
		if fs, e := webfs.GetFileStatus(gowfs.Path{Name: p}); e != nil {
			return nil, &os.PathError{
				Op:   "Stat",
				Path: p,
				Err:  os.ErrNotExist} //BUG(y): for whatever error, returns os.PathError.
		} else {
			mode, _ := strconv.ParseUint(fs.Permission, 8, 32)
			return &FileInfo{
				name: path.Base(p),
				size: fs.Length,
				mode: os.FileMode(mode),
				time: fs.ModificationTime,
				dir:  fs.Type == "DIRECTORY"}, nil
		}
	case HDFS:
		return rpcfs.Stat(p)
	case InMem:
		return DefaultInMemFS.Stat(p)
	default:
		return os.Stat(p)
	}
}
