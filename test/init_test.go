package test

import (
	"github.com/farseer-go/fs"
	"github.com/farseer-go/rabbit"
)

func init() {
	fs.Initialize[rabbit.Module]("test rabbit")
}
