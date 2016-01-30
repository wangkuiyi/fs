package fs

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const (
	testingContent = "Hello World!"
)

var (
	namenode = flag.String("fs.namenode", "", "Network address of HDFS namenode. Usually localhost:9000")
	webapi   = flag.String("fs.webapi", "", "Network address of WebHDFS server. Usually localhost:50070")
)

func init() {
	if e := HookupHDFS(*namenode, *webapi, ""); e != nil {
		log.Panicf("Failed connect to HDFS: %v", e)
	}
}

func testSuite(t *testing.T, protocol string) {
	dir := path.Join(protocol, fmt.Sprintf("tmp/test/github.com/wangkuiyi/fs/%v", time.Now().UnixNano()))
	file := path.Join(dir, "hello.txt")
	content := "Hello World!\n"
	assert := assert.New(t)

	ls, e := ReadDir(dir) // ReadDir on not existing dir
	assert.NotNil(e)
	assert.True(os.IsNotExist(e))
	assert.Equal(0, len(ls))

	_, e = Stat(file) // Stat on not existing file
	assert.NotNil(e)
	assert.True(os.IsNotExist(e))

	if assert.Nil(Mkdir(dir)) { // Mkdir
		ls, e := ReadDir(dir) // ReadDir on existing but empty dir
		assert.Nil(e)
		assert.Equal(0, len(ls))

		w, e := Create(file) // Create
		if assert.Nil(e) {
			fmt.Fprintf(w, content)
			w.Close()

			if protocol == "/webfs" {
				time.Sleep(time.Second / 2) // NOTE: WebHDFS API reacts slowly.
			}

			ls, e = ReadDir(dir) // ReadDir on existing and non-empty dir
			assert.Nil(e)
			assert.Equal(1, len(ls))

			_, e = Stat(file) // Stat on exisitng file
			assert.Nil(e)
			assert.False(os.IsNotExist(e))

			r, e := Open(file) // Read existing file
			if assert.Nil(e) {
				b, e := ioutil.ReadAll(r)
				assert.Nil(e)
				assert.Equal(string(b), content)
				r.Close()
			}
		}
	}
}

func TestWebFS(t *testing.T) {
	if len(*webapi) > 0 {
		testSuite(t, "/webfs")
	}
}
func TestHDFS(t *testing.T) {
	if len(*namenode) > 0 {
		testSuite(t, "/hdfs")
	}
}
func TestInMemFS(t *testing.T) {
	testSuite(t, "/inmem")
}
func TestLocalFS(t *testing.T) {
	testSuite(t, "/")
}
