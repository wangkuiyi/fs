package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"time"

	"github.com/stretchr/testify/assert"
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
