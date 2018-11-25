package diskv

import (
	"io/ioutil"
	"log"
	"os"
)

type Persister struct {
	stateFile string // store stateFile persistence data
}

func StartPersister(dir string) *Persister {
	ps := &Persister{}
	ps.createDirIfNotExists(dir)
	ps.stateFile = dir + "/state"
	return ps
}

// create directory if needed.
func (ps *Persister) createDirIfNotExists(dir string) {
	_, err := os.Stat(dir)
	if err != nil {
		if err := os.Mkdir(dir, 0777); err != nil {
			log.Fatalf("Mkdir(%v): %v", dir, err)
		}
	}
}

func (ps *Persister) filePut(content []byte) error {
	// overwrite
	return ioutil.WriteFile(ps.stateFile, content, 0666)
}

func (ps *Persister) fileGet() ([]byte, error) {
	return ioutil.ReadFile(ps.stateFile)
}
