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
		<description>Comma separated list of paths on the local filesystem of a DataNode where it should store its blocks.</description>
	  </property>
	  <property>
		<name>dfs.namenode.name.dir</name>
		<value>file:///Users/yiwang/hadoop/hdfs/namenode</value>
		<description>Path on the local filesystem where the NameNode stores the namespace and transaction logs persistently.</description>
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
// /Users/yiwang/hadoop. You might want to adapt the path to fit your
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
	hdfsPrefix  = "hdfs://"
	localPrefix = "file://"
	inmemPrefix = "inmem://"
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
	}
	return nil
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
	case strings.HasPrefix(name, localPrefix):
		f, e := os.Create(strings.TrimPrefix(name, localPrefix))
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
	case strings.HasPrefix(name, hdfsPrefix):
		go func() {
			_, e := hdfs.Create(r,
				gowfs.Path{Name: strings.TrimPrefix(name, hdfsPrefix)},
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
	case strings.HasPrefix(name, inmemPrefix):
		f := inmemfs.Create(strings.TrimPrefix(name, inmemPrefix))
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
	case strings.HasPrefix(name, localPrefix):
		f, e := os.Open(strings.TrimPrefix(name, localPrefix))
		if e != nil {
			return nil, CannotOpenFile
		}
		return f, nil
	case strings.HasPrefix(name, hdfsPrefix):
		r, e := hdfs.Open(
			gowfs.Path{Name: strings.TrimPrefix(name, hdfsPrefix)},
			0, 0, 0) // default offset, lenght and buffersize
		if e != nil {
			return nil, CannotOpenFile
		}
		return r, nil
	case strings.HasPrefix(name, inmemPrefix):
		r, e := inmemfs.Open(strings.TrimPrefix(name, inmemPrefix))
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
	case strings.HasPrefix(name, localPrefix):
		is, e := ioutil.ReadDir(strings.TrimPrefix(name, localPrefix))
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
	case strings.HasPrefix(name, hdfsPrefix):
		is, e := hdfs.ListStatus(gowfs.Path{Name: strings.TrimPrefix(name, hdfsPrefix)})
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
	case strings.HasPrefix(name, inmemPrefix):
		return nil, errors.New("in-memory filesystem does not yet support List")
	}
	return nil, UnknownFilesystemType
}
