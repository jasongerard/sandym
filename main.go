package main

import (
	"flag"
	"fmt"
	"os"
)

var helper *CassHelper

func fatalOnError(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {

	keyspace := flag.String("keyspace", "", "keyspace to use for migrations. required.")
	dir := flag.String("dir", ".", "location of migration files")
	hosts := flag.String("hosts", "localhost", "comma seperated list of cassandra instances")
	port := flag.Int("port", 9042, "port cassandra is listening on")

	flag.Parse()

	if *keyspace == "" {
		fmt.Print("keyspace required\n\n")
		flag.Usage()
		os.Exit(-1)
	}

	helper, err := NewCassHelper(*hosts, *port)
	fatalOnError(err)

	defer helper.Session().Close()

	helper.CreateKeyspace(*keyspace)

	err = helper.CreateSchemaTable(*keyspace)
	fatalOnError(err)

	err = RunMigrations(*keyspace, *dir, helper)
	fatalOnError(err)
}
