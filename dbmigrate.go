package dbmigrate

import (
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strconv"
)

type Config struct {
	Database       *sql.DB
	DatabaseSchema string
	ScriptsFolder  string
	Migrations     []Migration
	Log            *slog.Logger
}

type Migration struct {
	Version      int
	SQLStatement string
}

const migrationInfoTableName = "__dbmigrateinfo"

func Migrate(cfg Config) (err error) {
	if len(cfg.Migrations) == 0 {
		cfg.Migrations, err = loadMigrationFilesFromDir(cfg.ScriptsFolder)
		if err != nil {
			return err
		}
	}

	if err = createMigrationTable(cfg.Database); err != nil {
		return err
	}

	tx, err := startTransaction(cfg.Database)
	if err != nil {
		return err
	}

	if err = applyMigrations(tx, cfg.DatabaseSchema, cfg.Migrations); err != nil {
		return RollbackOnError(tx, err)
	}
	return tx.Commit()
}

func startTransaction(db *sql.DB) (*sql.Tx, error) {
	return db.Begin()
}

func RollbackOnError(tx *sql.Tx, err error) error {
	if err != nil {
		if innerErr := tx.Rollback(); innerErr != nil {
			return err
		}
		return err
	}
	return nil
}

func applyMigrations(tx *sql.Tx, schema string, migrations []Migration) error {
	dbVersion, err := getVersion(tx, schema)
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

		err = applyMigration(tx, m)
		if err != nil {
			return err
		}

		err = setVersion(tx, schema, m.Version)
		if err != nil {
			return err
		}
	}

	return nil
}

func applyMigration(tx *sql.Tx, migration Migration) error {
	_, err := tx.Exec(migration.SQLStatement)
	return err
}

func loadMigrationFilesFromDir(dir string) ([]Migration, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var migrations []Migration
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		version, err := extractVersionFromFilename(file.Name())
		if err != nil {
			return nil, fmt.Errorf("invalid file in migrations dir, does not start with a number: '%s'", file.Name())
		}

		sqlStatement, err := os.ReadFile(dir + "/" + file.Name())
		if err != nil {
			return nil, fmt.Errorf("failed to read file '%s': %s", file.Name(), err.Error())
		}

		migrations = append(migrations, Migration{
			Version:      version,
			SQLStatement: string(sqlStatement),
		})
	}

	return migrations, nil
}

func createMigrationTable(db *sql.DB) error {
	q := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s" (
		"schema" VARCHAR(100) PRIMARY KEY,
		"version" int NOT NULL
	)`, migrationInfoTableName)
	_, err := db.Exec(q)
	return err
}

func getVersion(tx *sql.Tx, name string) (int, error) {
	q := fmt.Sprintf(`SELECT "version" FROM "%s" WHERE "schema" = $1`, migrationInfoTableName)
	row := tx.QueryRow(q, name)
	if row.Err() != nil {
		return 0, row.Err()
	}
	version := -1
	err := row.Scan(&version)
	if err != nil {
		q = fmt.Sprintf(`INSERT INTO "%s" ("schema", "version") VALUES ($1, $2)`, migrationInfoTableName)
		_, err = tx.Exec(q, name, version)
		if err != nil {
			return 0, err
		}
	}
	return version, nil
}

func setVersion(tx *sql.Tx, name string, version int) error {
	q := fmt.Sprintf(`UPDATE "%s" SET "version" = %d WHERE "schema" = $1`, migrationInfoTableName, version)
	_, err := tx.Exec(q, name)
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
