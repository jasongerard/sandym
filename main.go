package main

import (
	"flag"
	"fmt"
	"log"
	"os"
)

var helper *CassHelper

func fatalOnError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func main() {

	keyspace := flag.String("keyspace", "", "keyspace to use for migrations. required.")
	dir := flag.String("dir", ".", "location of migration files")
	hosts := flag.String("hosts", "localhost", "comma seperated list of cassandra instances")
	port := flag.Int("port", 9042, "port cassandra is listening on")
	username := flag.String("username", "", "cassandra username")
	password := flag.String("password", "", "cassandra password")

	flag.Parse()

	if *keyspace == "" {
		fmt.Print("keyspace required\n\n")
		flag.Usage()
		os.Exit(-1)
	}

	casHelper, err := NewCassHelper(*hosts, *port, *keyspace, *username, *password)
	fatalOnError(err)

	versionHelper := NewVersionHelper(*keyspace, casHelper)

	defer casHelper.Close()

	versionHelper.CreateKeyspace()

	err = versionHelper.CreateSchemaTable()
	fatalOnError(err)

	err = RunMigrations(*dir, versionHelper)
	fatalOnError(err)
}
