package fs

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSaveAndLoad(t *testing.T) {
	assert := assert.New(t)

	type T struct {
		Name string
		Age  int
	}
	save := &T{Name: "Yi", Age: 36}

	filename := fmt.Sprintf("/inmem/test/github.com/wangkuiyi/fs-%v/saveLoad",
		time.Now().UnixNano())

	assert.Nil(Save(filename, save))

	load, e := Load(filename, &T{})
	assert.Equal(load, &T{Name: "Yi", Age: 36})
	assert.Nil(e)
}
