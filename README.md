# File

File is a file access package written in Go.  It can access files on
local filesystems, HDFS and an in-memory filesystem designed for unit
testing.

## Simple API

There are not APIs like Open, Read, Write, Close.  Instead, there are
basically only two functions in File:

  1. *Create* opens a new file or truncates an existing for writing.
  It returns an `io.WriteCloser`.  Close it after writing to identify
  the EOF.

  2. *Open* opens an exisiting file for reading.  It returns an
  `io.ReadCloser`.

## Examples

Please refer to http://godoc.org/github.com/wangkuiyi/file for
documents and examples.

## Accessing HDFS

I used to use https://github.com/zyxar/hdfs.go in accessing HDFS from
Go.  https://github.com/zyxar/hdfs.go is a CGO binding of
`libhdfs.so`, which in turn invokes JNI to access HDFS.  During the
process, it might create one or more Java threads.  Unfortunately,
these Java threads prevent `goprof` from profiling my Go programs that
use https://github.com/zyxar/hdfs.go.  This is because `goprof` has to
know the format of all stacks before it can take snapshots of these
stacks after every short time period, however, `goprof` knows only the
format of stacks corresponds to goroutines, but not those of Java
threads.

Luckily, recently versions of Hadoop provides Web API of HDFS access,
known as WebHDFS, where all operations on HDFS could be sent to HDFS
namenodes as HTTP requests.  This enables the development of HDFS
clients in various programming languages, and
https://github.com/vladimirvivien/gowfs is a Web HDFS client written
in Go.  File uses https://github.com/vladimirvivien/gowfs.
