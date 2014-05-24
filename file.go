// Package file provide uniform interface to access local filesystem,
// Hadoop filesystem (HDFS), and an in-memory filesystem define in
// https://github.com/wangkuiyi/file/inmemfs.  It uses WebHDFS
// interface defined in
// http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html
// to access HDFS.  In order to connect to HDFS, you need to manually
// invoke Initialize(), typically in main():
/*
	import (
	  "flag"
	  "github.com/wangkuiyi/file"
	)

	func main() {
	  flag.Parse()
	  file.Initialize()
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
		<value>hdfs://localhost/</value>
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
package file

import (
	"errors"
	"flag"
	"fmt"
	"github.com/vladimirvivien/gowfs"
	"github.com/wangkuiyi/file/inmemfs"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

const (
	HDFSPrefix  = "hdfs:"
	LocalPrefix = "file:"
	InMemPrefix = "inmem:"
)

var (
	namenode string
	hdfsUser string
	hdfs     *gowfs.FileSystem

	CannotOpenFile        = errors.New("Cannot open file.")
	UnknownFilesystemType = errors.New("Unknow filesystem type")
)

func init() {
	flag.StringVar(&namenode, "namenode", "",
		"HDFS namenode address. Empty for local access only.")
	flag.StringVar(&hdfsUser, "hdfsuser", "",
		"Empty for using OS login username.")
}

// Initialize prepares a client to the HDFS "namenode" in the role of
// "hdfsUser".  Also it tries to list files in the root directory to
// test the connection to the namenode.  This function is typically
// called in main() after the call of flag.Parse() parses flag
// "namenode" and "hdfsUser".
func Initialize() error {
	if len(namenode) > 0 {
		if len(hdfsUser) <= 0 {
			hdfsUser = os.Getenv("USER")
			if len(hdfsUser) <= 0 {
				return errors.New("No HDFS username specified")
			}
		}
		log.Printf("Connecting to HDFS %s@%s", hdfsUser, namenode)
		fs, e := gowfs.NewFileSystem(gowfs.Configuration{
			Addr: namenode, User: hdfsUser})
		if e != nil {
			return e
		}
		hdfs = fs
		return testConnection()
	} else {
		log.Printf("Command line flag namenode not specified, no HDFS access.")
	}
	return nil
}

// IsConnectedToHDFS returns if file has connected to HDFS namenode.
func IsConnectedToHDFS() bool {
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
	case strings.HasPrefix(name, LocalPrefix):
		f, e := os.Create(strings.TrimPrefix(name, LocalPrefix))
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
				log.Fatalf("Failed piping to file %s: %v", name, e)
			}
		}()
	case strings.HasPrefix(name, HDFSPrefix):
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
				log.Fatalf("Failed piping to file %s: %v", name, e)
			}
		}()
	case strings.HasPrefix(name, InMemPrefix):
		f := inmemfs.Create(strings.TrimPrefix(name, InMemPrefix))
		go func() {
			defer r.Close()
			_, e := io.Copy(f, r)
			if e != nil {
				log.Fatalf("Failed piping to file %s: %v", name, e)
			}
		}()
	default:
		r.Close()
		w.Close()
		return nil, UnknownFilesystemType
	}
	return w, nil
}

func Open(name string) (io.ReadCloser, error) {
	switch {
	case strings.HasPrefix(name, LocalPrefix):
		f, e := os.Open(strings.TrimPrefix(name, LocalPrefix))
		if e != nil {
			return nil, CannotOpenFile
		}
		return f, nil
	case strings.HasPrefix(name, HDFSPrefix):
		r, e := hdfs.Open(
			gowfs.Path{Name: strings.TrimPrefix(name, HDFSPrefix)},
			0, 0, 0) // default offset, lenght and buffersize
		if e != nil {
			return nil, CannotOpenFile
		}
		return r, nil
	case strings.HasPrefix(name, InMemPrefix):
		r, e := inmemfs.Open(strings.TrimPrefix(name, InMemPrefix))
		if e != nil {
			return nil, CannotOpenFile
		}
		return r, nil
	}
	return nil, UnknownFilesystemType
}

type Info struct {
	Name  string
	Size  int64
	IsDir bool
}

func List(name string) ([]Info, error) {
	switch {
	case strings.HasPrefix(name, LocalPrefix):
		is, e := ioutil.ReadDir(strings.TrimPrefix(name, LocalPrefix))
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
	case strings.HasPrefix(name, HDFSPrefix):
		is, e := hdfs.ListStatus(
			gowfs.Path{Name: strings.TrimPrefix(name, HDFSPrefix)})
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
	}
	return nil, UnknownFilesystemType
}

// Exists returns false, if there is any error.
func Exists(name string) (bool, error) {
	switch {
	case strings.HasPrefix(name, LocalPrefix):
		_, e := os.Stat(strings.TrimPrefix(name, LocalPrefix))
		if e != nil {
			if os.IsNotExist(e) {
				return false, nil
			} else {
				return false, fmt.Errorf("Exists(%s): %v", name, e)
			}
		}
		return true, nil
	case strings.HasPrefix(name, HDFSPrefix):
		fs := gowfs.FsShell{hdfs, "/"}
		// TODO(wyi): confirm that fs.Exists returns false when error.
		return fs.Exists(strings.TrimPrefix(name, HDFSPrefix))
	case strings.HasPrefix(name, InMemPrefix):
		return inmemfs.Exists(strings.TrimPrefix(name, InMemPrefix)),
			nil
	}
	return false, UnknownFilesystemType
}

// Create a directory, along with any necessary parents.  If the
// directory is already there, it returns nil.
//
// TODO(wyi): Add unit test for this function.
func MkDir(name string) error {
	switch {
	case strings.HasPrefix(name, LocalPrefix):
		return os.MkdirAll(strings.TrimPrefix(name, LocalPrefix), 0777)
	case strings.HasPrefix(name, HDFSPrefix):
		_, e := hdfs.MkDirs(
			gowfs.Path{Name: strings.TrimPrefix(name, HDFSPrefix)}, 0777)
		return e
	case strings.HasPrefix(name, InMemPrefix):
		inmemfs.MkDir(strings.TrimPrefix(name, InMemPrefix))
		return nil
	}
	return UnknownFilesystemType
}

// Put copy a local file to HDFS.  It overwrites if the destination
// already exists.
//
// BUG(wyi): hdfsPath must name a directory.  And due to a bug in
// "github.com/vladimirvivien/gowfs", this directory must not be the
// root directory "hdfs:/".
func Put(localFile, hdfsPath string) (bool, error) {
	if !strings.HasPrefix(localFile, LocalPrefix) {
		return false, fmt.Errorf("localFile %s has no LocalPrefix", localFile)
	}
	localFile = strings.TrimPrefix(localFile, LocalPrefix)

	if !strings.HasPrefix(hdfsPath, HDFSPrefix) {
		return false, fmt.Errorf("hdfsPath %s has no HDFSPrefix", hdfsPath)
	}
	hdfsPath = strings.TrimPrefix(hdfsPath, HDFSPrefix)

	fs := &gowfs.FsShell{hdfs, "/"}
	return fs.Put(localFile, hdfsPath, true)
}
