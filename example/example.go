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
	flag.Parse()
	fs.HookupHDFS(*namenode, *webhdfs, *user)

	dir := path.Join(fmt.Sprintf("/hdfs/tmp/test/github.com/wangkuiyi/file/%v", time.Now().UnixNano()))
	file := path.Join(dir, "hello.txt")
	content := "Hello World!\n"

	if e := fs.Mkdir(dir); e != nil {
		log.Panicf("Mkdir(%v) failed", dir)
	}

	if w, e := fs.Create(file); e != nil {
		log.Panicf("Create(%v) failed", file)
	} else {
		fmt.Fprintf(w, content)
		w.Close()
	}

	if _, e := fs.Stat(file); os.IsNotExist(e) {
		log.Panicf("Expecting file exists, but not")
	}

	if r, e := fs.Open(file); e == nil {
		b, _ := ioutil.ReadAll(r)
		fmt.Println(string(b))
		r.Close()
	}
}
