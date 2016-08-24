package main

import (
	"fmt"

	"log"
	"time"

	"github.com/gocql/gocql"
)

// VersionInfo blahs
type VersionInfo struct {
	Version int
	Hash    string
}

// CassHelper Basic wrapper for gocql
type CassHelper struct {
	Host    string
	Port    int
	session *gocql.Session
}

// Session gets the underlying session object
func (cas *CassHelper) Session() *gocql.Session {
	return cas.session
}

// KeyspaceExist checks for the existence of specified keyspace
func (cas *CassHelper) KeyspaceExist(keyspace string) bool {
	return cas.session.Query("select keyspace_name from system_schema.keyspaces where keyspace_name = ?", keyspace).Iter().NumRows() != 0
}

// TableExist checks for the existence of specified table
func (cas *CassHelper) TableExist(keyspace string, table string) bool {
	qstr := "select table_name from system_schema.tables where keyspace_name = ? and table_name = ?"

	i := cas.session.Query(qstr, keyspace, table).Iter()
	defer i.Close()

	return i.NumRows() != 0
}

// GetVersionInfo blah
func (cas *CassHelper) GetVersionInfo(keyspace string) []VersionInfo {
	qstr := fmt.Sprintf("select version, hash from %v.sandym_schema_version", keyspace)

	it := cas.session.Query(qstr).Iter()
	defer it.Close()

	numRows := it.NumRows()
	if numRows == 0 {
		return nil
	}

	infos := make([]VersionInfo, numRows)

	var version int
	var hash string

	for i := 0; i < numRows; i++ {
		it.Scan(&version, &hash)
		infos[i] = VersionInfo{Version: version, Hash: hash}
	}

	return infos

}

// GetCurrentVersion blah
func (cas *CassHelper) GetCurrentVersion(keyspace string) int {
	qstr := fmt.Sprintf("select max(version) from %v.sandym_schema_version", keyspace)

	it := cas.session.Query(qstr).Iter()
	defer it.Close()

	numRows := it.NumRows()
	if numRows == 0 {
		return 0
	}

	var version int

	it.Scan(&version)

	return version
}

// CreateSchemaTable creates the sandym_schema_version table if it doesn't exist
func (cas *CassHelper) CreateSchemaTable(keyspace string) error {

	if !cas.TableExist(keyspace, "sandym_schema_version") {
		qstr := fmt.Sprintf(`create table %v.sandym_schema_version (
			hash text,
			script_name text,
			ts timestamp,
			version int,
			primary key(hash)
		)`, keyspace)

		log.Println(qstr)
		return cas.session.Query(qstr).Exec()
	}
	return nil
}

// NewCassHelper blah
func NewCassHelper(host string, port int) *CassHelper {
	h := &CassHelper{Host: host, Port: port}

	cluster := gocql.NewCluster(host)
	cluster.Port = port
	cluster.ProtoVersion = 4
	cluster.Timeout = 2 * time.Second // default is 600ms, creating table timed out on that
	log.Printf("Timeout: %v", cluster.Timeout)

	s, err := cluster.CreateSession()

	if err != nil {
		panic(err)
	}

	h.session = s

	return h
}
