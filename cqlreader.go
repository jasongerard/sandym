package main

import (
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"log"
	"path/filepath"
	"regexp"
	"sort"
)

// Migration blah
type Migration struct {
	Version int
	Sha1    string
	Name    string
	Script  string
	Ignore  bool
}

// RunMigrations blah
func RunMigrations(keyspace string, dir string, cas *CassHelper) error {
	migrations := getMigrations(dir)

	if len(migrations) == 0 {
		log.Printf("No migrations found at %v\n", dir)
	}

	vis := cas.GetVersionInfo(keyspace)

	if vis != nil {
		// compare to files to determine what to run or if files have changes
		for _, v := range vis {
			for _, m := range migrations {
				if v.Version == m.Version {
					if v.Hash != m.Sha1 {
						log.Fatalf("Hash does not match for version %v %v", v.Version, m.Name)
					} else {
						m.Ignore = true
					}
				}
			}
		}
	}

	for _, m := range migrations {
		if m.Ignore {
			log.Printf("Skipping %v %v...", m.Version, m.Name)
			continue
		}
		log.Printf("Running migration %v", m.Name)
		err := cas.session.Query(m.Script).Exec()

		if err != nil {
			panic(err)
			return err
		}
	}

	return nil
}

// GetMigrations gets list of all migration files to run
func getMigrations(dir string) []Migration {
	files, _ := filepath.Glob(fmt.Sprintf("%v/*.cql", dir))
	sort.Strings(files)

	// remove elements that don't match regex
	for i := len(files) - 1; i >= 0; i-- {
		f := files[i]

		if match, _ := regexp.MatchString(`^\d*_\w*.cql$`, f); !match {
			files = append(files[:i], files[i+1:]...)
		}
	}

	if len(files) == 0 {
		return make([]Migration, 0)
	}

	migrations := make([]Migration, len(files))

	for i, f := range files {
		b, _ := ioutil.ReadFile(f)

		hasher := sha1.New()
		hasher.Write(b)
		sha := base64.URLEncoding.EncodeToString(hasher.Sum(nil))

		migrations[i] = Migration{Version: 0, Sha1: sha, Name: f, Script: string(b)}
	}

	return migrations
}
