package mscanner

import (
	"encoding/json"
	"io/ioutil"
	"log"

	"github.com/lcanal/mcollector/mmodels"
)

var (
	nodemodulesdir  string
	packagefilename string
)

func init() {
	nodemodulesdir = "node_modules"
	packagefilename = "package.json"
}

//ScanModules scan the node_modules directory
func ScanModules() []mmodels.NodeLibrary {
	var packageentries []mmodels.NodeLibrary

	dirs, err := ioutil.ReadDir(nodemodulesdir)
	if err != nil {
		log.Fatal(err)
	}
	for _, dir := range dirs {
		packagefilepath := nodemodulesdir + "/" + dir.Name() + "/" + packagefilename
		file, e := ioutil.ReadFile(packagefilepath)
		if e != nil {
			log.Printf("File error: %v\n", e)
		}

		var pe mmodels.NodeLibrary
		ue := json.Unmarshal(file, &pe)
		if ue != nil {
			log.Printf("Error unmarshalling %v\n", ue)
			continue
		}

		packageentries = append(packageentries, pe)
	}

	return packageentries
}
