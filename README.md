# `fs`

`fs` is a Go API with the same syntax and semantic as standard package
`os` for accessing

1. the local filesystem,
1. HDFS via Hadoop WebHDFS API,
1. HDFS via Hadoop 2.2.x native protobuf-based RPC, and
1. an in-memory filesystem for unit testing

Documentation is at http://godoc.org/github.com/wangkuiyi/fs.

Run `go get github.com/wangkuiyi/fs` to install.


## Convention

1. `/hdfs/home/you` refers to path `/home/you` on HDFS and accessed via Hadoop native RPC.
1. `/webfs/home/you` refers to the same path on HDFS but accessed via WebHDFS.
1. `/inmem/home/you` refers to `/home/you` on the in-memory filesystem.
1. `/home/you` refers to `/home/you` on the local filesystem.


## Usage

The following example comes from `example/example.go`.  It shows how
`fs` hooks up with HDFS using `fs.HookupHDFS`.

```
func main() {
	namenode := flag.String("namenode", "localhost:9000", "HDFS namenode address.")
	webhdfs := flag.String("webhdfs", "localhost:50070", "WebHDFS address.")
	user := flag.String("user", "", "HDFS username. Could be empty.")
	flag.Parse()
	fs.HookupHDFS(*namenode, *webhdfs, *user)

	dir := path.Join(fmt.Sprintf("/hdfs/tmp/test/github.com/wangkuiyi/file/%v", time.Now().UnixNano()))
	file := path.Join(dir, "hello.txt")
	content := "Hello World!\n"

	if e := fs.Mkdir(dir); e == nil {
		if w, e := fs.Create(file); e == nil {
			fmt.Fprintf(w, content)
			w.Close()

			_, e = Stat(file) // Stat on not existing file
			assert.NotNil(e)
			assert.True(os.IsNotExist(e))

			if r, e := fs.Open(file); e == nil {
				b, _ := ioutil.ReadAll(r)
				fmt.Println(string(b))
				r.Close()
			}
		}
	}
}
```

## Internals

I used to use [hdfs.go](https://github.com/zyxar/hdfs.go) for access
HDFS.  [hdfs.go](https://github.com/zyxar/hdfs.go) is a CGO binding of
`libhdfs.so`, which in turn invokes JNI to access HDFS.  This
invocation often creates some Java threads as a side-effect.
Unfortunately, these Java threads prevent `goprof` from profiling the
Go programs, because `goprof` doesn't understand the format of Java
threads and thus cannot take stack snapshots.

[WebHDFS](http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html)
is my second trial.  `fs` uses WebHDFS clients
[gowfs](https://github.com/vladimirvivien/gowfs).  But WebHDFS has a
delay problem.  Say, if you list the directory immediately after
creating a file, it is often that the newly created file is not in the
list.  Therefore, it is highly recommended to use the native
protobuf-based RPC system.
   
