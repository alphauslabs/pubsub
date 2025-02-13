package main

import (
	"flag"

	"github.com/golang/glog"
)

func main() {
	flag.Set("logtostderr", "true")
	flag.Parse()
	glog.Info("Hello, World!")
}
