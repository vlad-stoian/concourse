package migration

import (
	"database/sql"
	"fmt"
	"github.com/concourse/voyager"
	"strings"
	"errors"
)

func NewSchemaAdapter(oldLastVersion int, newFirstVersion int) voyager.SchemaAdapter {
	return &schemaAdapter{
		schemaMigrationsStartVersion:     oldLastVersion,
		newMigrationsHistoryFirstVersion: newFirstVersion,
	}
}

type schemaAdapter struct {
	// from when we started using github.com/mattes/migrate (v3.7.0)
	schemaMigrationsStartVersion int
	// from when we started using voyager (v4.1.0)
	newMigrationsHistoryFirstVersion int
}

func (adapter *schemaAdapter) MigrateFromOldSchema(db *sql.DB, toVersion int) (int, error) {
	//if toVersion >= adapter.newMigrationsHistoryFirstVersion {
	//	return adapter.newMigrationsHistoryFirstVersion, nil
	//}

	oldSchemaExists, err := checkTableExist(db, "schema_migrations")
	if err != nil {
		return -1, err
	}

	if !oldSchemaExists {
		return 0, nil
	}

	var (
		currentVersion int
		dirty          bool
	)
	row := db.QueryRow("SELECT * FROM schema_migrations")
	err = row.Scan(&currentVersion, &dirty)
	if dirty == true {
		return 0, fmt.Errorf("db version %d is in a dirty state", currentVersion)
	}
	if currentVersion < adapter.schemaMigrationsStartVersion {
		return 0, fmt.Errorf("must migrate from db version %d or higher (Concourse v3.7.0 or higher), current db version: %d", adapter.schemaMigrationsStartVersion, currentVersion)
	}

	return currentVersion, nil
}

// this does not support migrations from v3.6.0 and prior
func (adapter schemaAdapter) MigrateToOldSchema(db *sql.DB, toVersion int) error {
	if toVersion >= adapter.newMigrationsHistoryFirstVersion {
		return nil
	}

	if toVersion < adapter.schemaMigrationsStartVersion {
		return fmt.Errorf("cannot migrate down below db version %d (Concourse v3.7.0), attempted version: %d", adapter.schemaMigrationsStartVersion, toVersion)
	}

	oldSchemaExists, err := checkTableExist(db, "schema_migrations")
	if err != nil {
		return err
	}

	if !oldSchemaExists {
		_, err := db.Exec("CREATE TABLE schema_migrations (version bigint, dirty boolean)")
		if err != nil {
			return err
		}

		_, err = db.Exec("INSERT INTO schema_migrations (version, dirty) VALUES ($1, false)", toVersion)
		if err != nil {
			return err
		}
	} else {
		_, err := db.Exec("UPDATE schema_migrations SET version=$1, dirty=false", toVersion)
		if err != nil {
			return err
		}
	}

	return nil
}

func (adapter *schemaAdapter) CurrentVersion(db *sql.DB) (int, error) {
	oldSchemaExists, err := checkTableExist(db, "schema_migrations")
	if err != nil {
		return 0, err
	}

	if !oldSchemaExists {
		return 0, nil
	}

	var (
		version int
		dirty bool
	)
	row := db.QueryRow("SELECT * FROM schema_migrations")
	err = row.Scan(&version, &dirty)
	if err != nil {
		return 0, err
	}

	if dirty {
		return 0, errors.New("cannot determine current db migration version. Database is in a dirty state")
	}

	return version, nil
}

func checkTableExist(db *sql.DB, tableName string) (bool, error) {
	var exists bool
	err := db.QueryRow("SELECT EXISTS ( SELECT 1 FROM information_schema.tables WHERE table_name=$1)", tableName).Scan(&exists)
	if err != nil {
		return false, err
	}

	if exists {
		return true, nil
	}

	// SELECT EXISTS doesn't fail if the user doesn't have permission to look
	// at the information_schema, so fall back to checking the table directly
	rows, err := db.Query("SELECT * from " + tableName)
	if rows != nil {
		defer rows.Close()
	}

	if err == nil {
		return true, nil
	}

	if strings.Contains(err.Error(), "does not exist") {
		return false, nil
	} else {
		return false, err
	}
}
