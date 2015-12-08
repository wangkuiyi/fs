package fs

import (
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

func init() {
	if os.Getenv("DISABLE_HDFS_TEST") == "" {
		if e := HookupHDFS("localhost:9000", "localhost:50070", ""); e != nil {
			log.Panicf("Failed connect to HDFS: %v", e)
		}
	}
}

func testSuite(t *testing.T, protocol string) {
	dir := path.Join(protocol, fmt.Sprintf("tmp/test/github.com/wangkuiyi/file/%v", time.Now().Unix()))
	file := path.Join(dir, "hello.txt")
	content := "Hello World!\n"
	assert := assert.New(t)

	ls, e := ReadDir(dir) // ReadDir on not existing dir
	assert.NotNil(e)
	assert.Equal(len(ls), 0)

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

			ls, e = ReadDir(dir) // ReadDir on existing and non-empty dir
			assert.Nil(e)
			assert.Equal(len(ls), 1)

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

func TestHDFS(t *testing.T) {
	testSuite(t, "/hdfs")
}

// TODO(y): Fix the following failing test.
/*
func TestWebFS(t *testing.T) {
	// TODO(y): Fix the following failing test.
	// testSuite(t, "/webfs")
}
*/

func TestInMemFS(t *testing.T) {
	testSuite(t, "/inmem")
}

func TestLocalFS(t *testing.T) {
	testSuite(t, "/")
}
