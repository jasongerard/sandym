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
	"time"
)

// Migration blah
type Migration struct {
	Sha1       string
	Name       string
	Script     string
	Ignore     bool
	ScriptName string
}

// RunMigrations blah
func RunMigrations(dir string, vh *VersionHelper) error {
	migrations := getMigrations(dir)

	if len(migrations) == 0 {
		log.Printf("No migrations found at %v\n", dir)
	}

	vis := vh.GetVersionInfo()

	if vis != nil {
		// compare to files to determine what to run or if files have changes
		for _, v := range vis {
			for _, m := range migrations {
				if v.ScriptName == m.ScriptName {
					if v.Hash != m.Sha1 {
						log.Fatalf("Hashes do not match for script %v.\nFile hash %v\nStored Hash %v", m.Name, m.Sha1, v.Hash)
					} else {
						m.Ignore = true
					}
				}
			}
		}
	}

	for _, m := range migrations {
		if m.Ignore == true {
			log.Printf("Skipping %v with hash %v\n", m.Name, m.Sha1)
			continue
		}
		log.Printf("Running migration %v", m.Name)
		err := vh.cas.ExecMultiple(m.Script)

		if err != nil {
			return err
		}

		insert := fmt.Sprintf(`insert into %v.sandym_schema_version (hash, script_name, ts) 
							values (?, ?, ?)`, vh.Keyspace())

		err = vh.cas.Exec(insert, m.Sha1, m.Name, time.Now())

		if err != nil {
			return err
		}
	}

	return nil
}

// GetMigrations gets list of all migration files to run
func getMigrations(dir string) []*Migration {
	files, _ := filepath.Glob(fmt.Sprintf("%v/*.cql", dir))
	sort.Strings(files)

	// remove elements that don't match regex
	for i := len(files) - 1; i >= 0; i-- {
		f := files[i]

		if match, _ := regexp.MatchString(`^\d*_{1,2}\w*.cql$`, filepath.Base(f)); !match {
			files = append(files[:i], files[i+1:]...)
		}
	}

	if len(files) == 0 {
		return make([]*Migration, 0)
	}

	migrations := make([]*Migration, len(files))

	for i, f := range files {
		b, _ := ioutil.ReadFile(f)

		hasher := sha1.New()
		hasher.Write(b)
		sha := base64.URLEncoding.EncodeToString(hasher.Sum(nil))

		migrations[i] = &Migration{Sha1: sha, Name: f, Script: string(b), ScriptName: f}
	}

	return migrations
}
