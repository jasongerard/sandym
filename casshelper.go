package main

import (
	"errors"
	"strings"
	"time"

	"github.com/gocql/gocql"
)

// CassHelper Basic wrapper for gocql
type CassHelper struct {
	Host    string
	Port    int
	session *gocql.Session
}

func (cas *CassHelper) ExecMultiple(query string, v ...interface{}) error {

	if strings.TrimSpace(query) == "" {
		return errors.New("query cannot be empty")
	}
	qs := strings.Split(query, ";")

	if len(qs) == 0 {
		return errors.New("query cannot be empty")
	}

	for _, q := range qs {
		if strings.TrimSpace(q) == "" {
			continue
		}
		err := cas.Exec(strings.TrimSpace(q), v...)

		if err != nil {
			return err
		}
	}
	return nil
}

func (cas *CassHelper) Exec(query string, v ...interface{}) error {

	if strings.TrimSpace(query) == "" {
		return errors.New("query cannot be empty")
	}

	return cas.session.Query(query, v...).Exec()
}

func (cas *CassHelper) Query(query string, v ...interface{}) *gocql.Query {

	return cas.session.Query(query, v...)
}

func (cas *CassHelper) Close() {
	cas.session.Close()
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

// NewCassHelper blah
func NewCassHelper(host string, port int, keyspace string, username string, password string) (*CassHelper, error) {
	h := &CassHelper{Host: host, Port: port}

	cluster := gocql.NewCluster(host)
	cluster.Keyspace = keyspace
	cluster.Port = port
	cluster.ProtoVersion = 4
	cluster.Timeout = 2 * time.Second // default is 600ms, creating table timed out on that

	if username != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: username,
			Password: password,
		}
	}

	s, err := cluster.CreateSession()

	if err != nil {
		return nil, err
	}

	h.session = s

	return h, nil
}
