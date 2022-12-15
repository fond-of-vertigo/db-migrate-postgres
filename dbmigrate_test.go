package dbmigrate

import "testing"

func TestMigrate(t *testing.T) {
	db := mustGetCleanTestDB(t, "testabc")
	defer db.Close()
}
