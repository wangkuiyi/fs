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
	"os"
	"strings"
	"time"
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
