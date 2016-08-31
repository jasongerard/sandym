package main

import (
	"fmt"

	"time"

	"github.com/gocql/gocql"
)

// VersionInfo blahs
type VersionInfo struct {
	Hash       string
	ScriptName string
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
	qstr := fmt.Sprintf("select hash, script_name from %v.sandym_schema_version", keyspace)

	it := cas.session.Query(qstr).Iter()
	defer it.Close()

	numRows := it.NumRows()
	if numRows == 0 {
		return nil
	}

	infos := make([]VersionInfo, numRows)

	var scriptName string
	var hash string

	for i := 0; i < numRows; i++ {
		it.Scan(&hash, &scriptName)
		infos[i] = VersionInfo{ScriptName: scriptName, Hash: hash}
	}

	return infos

}

func (cas *CassHelper) CreateKeyspace(keyspace string) error {

	if !cas.KeyspaceExist(keyspace) {
		qstr := fmt.Sprintf(`CREATE KEYSPACE %v WITH 
		replication = {
			'class': 'SimpleStrategy', 
			'replication_factor': '1'
		} AND durable_writes = true;`, keyspace)

		return cas.session.Query(qstr).Exec()
	}

	return nil
}

// CreateSchemaTable creates the sandym_schema_version table if it doesn't exist
func (cas *CassHelper) CreateSchemaTable(keyspace string) error {

	if !cas.TableExist(keyspace, "sandym_schema_version") {
		qstr := fmt.Sprintf(`create table %v.sandym_schema_version (
			hash text,
			script_name text,
			ts timestamp,
			primary key(hash)
		)`, keyspace)

		return cas.session.Query(qstr).Exec()
	}
	return nil
}

// NewCassHelper blah
func NewCassHelper(host string, port int) (*CassHelper, error) {
	h := &CassHelper{Host: host, Port: port}

	cluster := gocql.NewCluster(host)
	cluster.Port = port
	cluster.ProtoVersion = 4
	cluster.Timeout = 2 * time.Second // default is 600ms, creating table timed out on that

	s, err := cluster.CreateSession()

	if err != nil {
		return nil, err
	}

	h.session = s

	return h, nil
}
