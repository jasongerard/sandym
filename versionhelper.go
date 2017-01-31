package main

import "fmt"

// VersionInfo blahs
type VersionInfo struct {
	Hash       string
	ScriptName string
}

type VersionHelper struct {
	keyspace string
	cas      *CassHelper
}

func NewVersionHelper(keyspace string, cas *CassHelper) *VersionHelper {
	return &VersionHelper{cas: cas, keyspace: keyspace}
}

func (v *VersionHelper) Keyspace() string {
	return v.keyspace
}

// GetVersionInfo blah
func (v *VersionHelper) GetVersionInfo() []VersionInfo {
	qstr := fmt.Sprintf("select hash, script_name from %v.sandym_schema_version", v.keyspace)

	it := v.cas.Query(qstr).Iter()
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

func (v *VersionHelper) CreateKeyspace() error {

	if !v.cas.KeyspaceExist(v.keyspace) {
		qstr := fmt.Sprintf(`CREATE KEYSPACE %v WITH 
		replication = {
			'class': 'SimpleStrategy', 
			'replication_factor': '1'
		} AND durable_writes = true;`, v.keyspace)

		return v.cas.Exec(qstr)
	}

	return nil
}

// CreateSchemaTable creates the sandym_schema_version table if it doesn't exist
func (v *VersionHelper) CreateSchemaTable() error {

	if !v.cas.TableExist(v.keyspace, "sandym_schema_version") {
		qstr := fmt.Sprintf(`create table %v.sandym_schema_version (
			hash text,
			script_name text,
			ts timestamp,
			primary key(hash)
		);`, v.keyspace)

		return v.cas.Exec(qstr)
	}
	return nil
}
