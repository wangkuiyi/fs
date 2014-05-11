package file

import (
	"encoding/json"
	"os"
	"testing"
)

const (
	testingContent = "Hello World!"
)

func TestInitialize(t *testing.T) {
	if os.Getenv("DISABLE_HDFS_TEST") != "" {
		t.SkipNow()
		return
	}
	namenode = "localhost:50070"
	if e := Initialize(); e != nil {
		t.Errorf("Failed connect to HDFS: %v", e)
	}
}

func ExampleCreate(name string, t *testing.T) {
	w, e := Create(name)
	if e != nil {
		t.Fatalf("Create failed: %v", e)
	}
	defer w.Close()

	en := json.NewEncoder(w)
	e = en.Encode(testingContent)
	if e != nil {
		t.Errorf("Failed encoding: %v", e)
	}
}

func ExampleOpen(name string, t *testing.T) {
	r, e := Open(name)
	if e != nil {
		t.Fatalf("Open failed: %v", e)
	}
	defer r.Close()

	var m string
	de := json.NewDecoder(r)
	e = de.Decode(&m)
	if e != nil {
		t.Errorf("Failed decoding: %v", e)
	}
	if m != testingContent {
		t.Errorf("Expecting %s, got %s", testingContent, m)
	}
}

func TestCreateLocal(t *testing.T) {
	ExampleCreate("file:///tmp/b", t)
}

func TestCreateHDFS(t *testing.T) {
	if os.Getenv("DISABLE_HDFS_TEST") != "" {
		t.SkipNow()
		return
	}
	ExampleCreate("hdfs:///tmpb", t)
}

func TestCreateInMem(t *testing.T) {
	ExampleCreate("inmem://tmp/b", t)
}

func TestOpenLocal(t *testing.T) {
	ExampleCreate("file:///tmp/b", t)
	ExampleOpen("file:///tmp/b", t)
}

func TestOpenHDFS(t *testing.T) {
	if os.Getenv("DISABLE_HDFS_TEST") != "" {
		t.SkipNow()
		return
	}
	ExampleCreate("hdfs:///tmpb", t)
	ExampleOpen("inmem://tmp/b", t)
}

func TestOpenInMem(t *testing.T) {
	ExampleCreate("inmem://tmp/b", t)
	ExampleOpen("inmem://tmp/b", t)
}