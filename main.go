package main

import (
	"flag"
	"log"
)

var helper *CassHelper

func fatalOnError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func main() {

	dir := flag.String("dir", ".", "location of migration files")
	hosts := flag.String("hosts", "localhost", "comma seperated list of cassandra instances")
	port := flag.Int("port", 9042, "port cassandra is listening on")

	flag.Parse()

	helper = NewCassHelper(*hosts, *port)
	defer helper.Session().Close()

	keyspace := "randrr"

	err := helper.CreateSchemaTable(keyspace)
	fatalOnError(err)

	log.Printf("version %v", helper.GetCurrentVersion(keyspace))

	err = RunMigrations(keyspace, *dir, helper)
	fatalOnError(err)

	log.Printf("version %v", helper.GetCurrentVersion(keyspace))

}
