package main

import (
	"./router"
	"runtime/debug"
)

func main() {
	debug.SetGCPercent(50)
	r := router.Create()
	r.Handler()
}
