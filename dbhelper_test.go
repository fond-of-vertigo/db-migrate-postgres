package dbmigrate

import (
	"database/sql"
	"fmt"
	"net/url"
	"testing"

	_ "github.com/lib/pq"
)

const testDBAdminUsername string = "dbmigrate"
const testDBAdminPassword string = "dbmigrate"

func mustGetCleanTestDB(t *testing.T, schema string) *sql.DB {
	db, err := openTestDB(testDBAdminUsername, testDBAdminPassword)
	if err != nil {
		t.Fatal(err)
	}

	// Create a clean schema. Schema name = user = password
	dbMustExec(db,
		fmt.Sprintf("DROP SCHEMA IF EXISTS \"%s\" CASCADE", schema),
		fmt.Sprintf("DROP USER IF EXISTS \"%s\"", schema),
		fmt.Sprintf("CREATE USER \"%s\" WITH PASSWORD '%s'", schema, schema),
		fmt.Sprintf("GRANT \"%s\" TO \"%s\"", schema, testDBAdminUsername),
		fmt.Sprintf("CREATE SCHEMA \"%s\" AUTHORIZATION \"%s\"", schema, schema),
		fmt.Sprintf("ALTER USER \"%s\" SET SEARCH_PATH = '%s'", schema, schema), // set default schema
	)

	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}

	db, err = openTestDB(schema, schema)
	if err != nil {
		t.Fatal(err)
	}

	return db
}

func dbMustExec(db *sql.DB, sqls ...string) {
	for _, sql := range sqls {
		_, err := db.Exec(sql)
		if err != nil {
			db.Close()
			panic(fmt.Sprintf("SQL command failed:\n%s\n%s", sql, err.Error()))
		}
	}
}

func openTestDB(username, password string) (*sql.DB, error) {
	dbname := "dbmigrate"
	dbhost := "localhost"
	options := url.Values{"sslmode": []string{"disable"}}
	connStr := fmt.Sprintf("postgres://%s:%s@%s/%s?%s", username, password, dbhost, dbname, options.Encode())
	return sql.Open("postgres", connStr)
}
