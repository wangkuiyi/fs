package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"time"

	"github.com/wangkuiyi/fs"
)

func main() {
	namenode := flag.String("namenode", "localhost:9000", "HDFS namenode address.")
	webhdfs := flag.String("webhdfs", "localhost:50070", "WebHDFS address.")
	user := flag.String("user", "", "HDFS username. Could be empty.")
	prefix := flag.String("dir", "/tmp/test/github.com/wangkuiyi/file", "A test directory to be created.")
	flag.Parse()
	fs.HookupHDFS(*namenode, *webhdfs, *user)

	dir := fmt.Sprintf("%s/%v", *prefix, time.Now().UnixNano())
	file := path.Join(dir, "hello.txt")
	content := "Hello World!\n"

	if e := fs.Mkdir(dir); e != nil {
		log.Panicf("Mkdir(%v) failed: %v", dir, e)
	}

	if w, e := fs.Create(file); e != nil {
		log.Panicf("Create(%v) failed: %v", file, e)
	} else {
		fmt.Fprintf(w, content)
		w.Close()
	}

	if _, e := fs.Stat(file); os.IsNotExist(e) {
		log.Panicf("Expecting file exists, but not: %v", e)
	}

	if r, e := fs.Open(file); e == nil {
		b, _ := ioutil.ReadAll(r)
		fmt.Println(string(b))
		r.Close()
	}
}
