# File

File is a file access package written in Go.  It can access files on

1. local filesystems,
2. HDFS, and
3. an [in-memory filesystem](https://github.com/wangkuiyi/file/tree/master/inmemfs) designed for unit testing.

## Simple API

There are not APIs like Open, Read, Write, Close.  Instead, there are
basically only two functions in File:

  1. **Create** opens a new file or truncates an existing for writing.
  It returns an `io.WriteCloser`.  Close it after writing to identify
  the EOF.

  2. **Open** opens an exisiting file for reading.  It returns an
  `io.ReadCloser`.

## Examples

Please refer to http://godoc.org/github.com/wangkuiyi/file for
documents and examples.

## WebHDFS

I used to use [hdfs.go](https://github.com/zyxar/hdfs.go) in accessing
HDFS from Go.  [hdfs.go](https://github.com/zyxar/hdfs.go) is a CGO
binding of `libhdfs.so`, which in turn invokes JNI to access HDFS.
During the process, it might create one or more Java threads.
Unfortunately, these Java threads prevent `goprof` from profiling my
Go programs that use [hdfs.go](https://github.com/zyxar/hdfs.go).
This is because `goprof` has to know the format of all stacks before
it can take snapshots of these stacks after every short time period,
however, `goprof` knows only the format of stacks corresponds to
goroutines, but not those of Java threads.

Luckily, recently versions of Hadoop provides Web API of HDFS, known
as
[WebHDFS](http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html).
This enables the development of HDFS clients in various programming
languages, and [gowfs](https://github.com/vladimirvivien/gowfs) is a
Web HDFS client written in Go.  File uses
[gowfs](https://github.com/vladimirvivien/gowfs).

## Install

Installation is very simple.  After setting environment variable
`GOPATH`, checkout most recent source code using `go get`:

    go get github.com/wangkuiyi/file

You can run unit tests by

    go test

This tests operations on local filesystems, HDFS and in-memory
filesystem.  If you have not yet set up an HDFS on localhost, you
might want to disable operations on HDFS by:

    DISABLE_HDFS_TEST go test

For how to setup an HDFS for development and test, please refer to
http://godoc.org/github.com/wangkuiyi/file.
