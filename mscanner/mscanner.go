package mscanner

import (
	"fmt"
	"io/ioutil"
	"log"
)

//ScanModules scan the node_modules directory
func ScanModules() {
	files, err := ioutil.ReadDir("node_modules")
	if err != nil {
		log.Fatal(err)
	}
	for _, f := range files {
		fmt.Println(f.Name())
	}
}
