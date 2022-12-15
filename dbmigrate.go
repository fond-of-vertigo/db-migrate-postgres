package dbmigrate

import (
	"database/sql"
	"fmt"
	"os"
	"sort"
	"strconv"
)

type Config struct {
	DB            *sql.DB
	ScriptsFolder string
	SchemaName    string
	Migrations    []*Migration
}

type Migration struct {
	Version int
	SQL     string
}

const versionTableName = "__dbmigrateinfo"

func Migrate(cfg *Config) (err error) {
	if len(cfg.Migrations) == 0 {
		cfg.Migrations, err = loadMigrationFiles(cfg.ScriptsFolder)
		if err != nil {
			return err
		}
	}

	err = createMigrationTable(cfg.DB)
	if err != nil {
		return err
	}

	err = lockTable(cfg.DB)
	if err != nil {
		return err
	}

	defer unlockTable(cfg.DB, &err)

	err = applyMigrations(cfg.DB, cfg.SchemaName, cfg.Migrations)
	return err
}

func applyMigrations(db *sql.DB, schema string, migrations []*Migration) error {
	dbVersion, err := getVersion(db, schema)
	if err != nil {
		return err
	}

	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].Version < migrations[j].Version
	})

	for _, m := range migrations {
		if m.Version <= dbVersion {
			continue
		}

		err = applyMigration(db, schema, m)
		if err != nil {
			return err
		}

		err = setVersion(db, schema, m.Version)
		if err != nil {
			return err
		}
	}

	return nil
}

func applyMigration(db *sql.DB, schema string, migration *Migration) error {
	_, err := db.Exec(migration.SQL)
	return err
}

func loadMigrationFiles(folder string) ([]*Migration, error) {
	files, err := os.ReadDir(folder)
	if err != nil {
		return nil, err
	}

	migrations := []*Migration{}
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		version, err := extractVersionFromFilename(file.Name())
		if err != nil {
			return nil, fmt.Errorf("invalid file in migrations folder, does not start with a number: '%s'", file.Name())
		}

		data, err := os.ReadFile(folder + "/" + file.Name())
		if err != nil {
			return nil, fmt.Errorf("failed to read file '%s': %s", file.Name(), err.Error())
		}

		migrations = append(migrations, &Migration{
			Version: int(version),
			SQL:     string(data),
		})
	}

	return migrations, nil
}

func createMigrationTable(db *sql.DB) error {
	sql := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s" (
		"schema" VARCHAR(100) PRIMARY KEY,
		"version" int NOT NULL
	)`, versionTableName)
	_, err := db.Exec(sql)
	return err
}

func lockTable(db *sql.DB) error {
	_, err := db.Exec("BEGIN")
	if err != nil {
		return err
	}

	_, err = db.Exec(fmt.Sprintf(`LOCK TABLE "%s" IN ACCESS EXCLUSIVE MODE NOWAIT`, versionTableName))
	return err
}

func unlockTable(db *sql.DB, lastErr *error) error {
	sql := "COMMIT"
	if lastErr != nil && *lastErr != nil {
		sql = "ROLLBACK"
	}

	_, err := db.Exec(sql)
	return err
}

func getVersion(db *sql.DB, name string) (int, error) {
	sql := fmt.Sprintf(`SELECT "version" FROM "%s" WHERE "schema" = $1`, versionTableName)
	row := db.QueryRow(sql, name)
	if row.Err() != nil {
		return 0, row.Err()
	}
	version := -1
	err := row.Scan(&version)
	if err != nil {
		sql = fmt.Sprintf(`INSERT INTO "%s" ("schema", "version") VALUES ($1, $2)`, versionTableName)
		_, err = db.Exec(sql, name, version)
		if err != nil {
			return 0, err
		}
	}
	return version, nil
}

func setVersion(db *sql.DB, name string, version int) error {
	sql := fmt.Sprintf(`UPDATE "%s" SET "version" = %d WHERE "schema" = $1`, versionTableName, version)
	_, err := db.Exec(sql, name)
	return err
}

func extractVersionFromFilename(name string) (int, error) {
	if len(name) == 0 {
		return 0, fmt.Errorf("file name is empty")
	}

	var i int
	for i = 0; i < len(name) && name[i] >= '0' && name[i] <= '9'; i++ {
	}

	if i == 0 {
		return 0, fmt.Errorf("invalid file name: does not start with a number: '%s'", name)
	}

	return strconv.Atoi(name[:i])
}
