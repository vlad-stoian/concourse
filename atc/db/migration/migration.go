package migration

import (
	"database/sql"
	"fmt"
	"strings"

	"code.cloudfoundry.org/lager"
	"github.com/concourse/concourse/atc/db/encryption"
	"github.com/concourse/concourse/atc/db/lock"
	"github.com/concourse/concourse/atc/db/migration/migrations"
	"github.com/concourse/voyager"
	"github.com/gobuffalo/packr"
	_ "github.com/lib/pq"
)

func NewOpenHelper(driver, name string, lockFactory lock.LockFactory, strategy encryption.Strategy) *OpenHelper {
	logger := lager.NewLogger("migrations")
	source := &packrSource{packr.NewBox("./migrations")}
	lockID := lock.NewDatabaseMigrationLockID()
	goMigrationsRunner := migrations.NewMigrations(strategy)

	schemaAdapter := &schemaAdapter{
		schemaMigrationsStartVersion:     1510262030,
		newMigrationsHistoryFirstVersion: 1532706545,
	}

	migrator := voyager.NewMigrator(
		logger,
		lockID[0],
		source,
		goMigrationsRunner,
		schemaAdapter,
	)

	return &OpenHelper{
		driver,
		name,
		lockFactory,
		strategy,
		migrator,
	}
}

type OpenHelper struct {
	driver         string
	dataSourceName string
	lockFactory    lock.LockFactory
	strategy       encryption.Strategy
	migrator       voyager.Migrator
}

func (self *OpenHelper) CurrentVersion() (int, error) {
	db, err := sql.Open(self.driver, self.dataSourceName)
	if err != nil {
		return -1, err
	}

	defer db.Close()
	return self.migrator.CurrentVersion(db)
}

func (self *OpenHelper) SupportedVersion() (int, error) {
	return self.migrator.SupportedVersion()
}

func (self *OpenHelper) Open() (*sql.DB, error) {
	db, err := sql.Open(self.driver, self.dataSourceName)
	if err != nil {
		return nil, err
	}

	if err := self.migrator.Up(db); err != nil {
		_ = db.Close()
		return nil, err
	}

	return db, nil
}

func (self *OpenHelper) OpenAtVersion(version int) (*sql.DB, error) {
	db, err := sql.Open(self.driver, self.dataSourceName)
	if err != nil {
		return nil, err
	}

	if err := self.migrator.Migrate(db, version); err != nil {
		_ = db.Close()
		return nil, err
	}

	return db, nil
}

func (self *OpenHelper) MigrateToVersion(version int) error {
	db, err := sql.Open(self.driver, self.dataSourceName)
	if err != nil {
		return err
	}

	defer db.Close()

	err = self.migrateFromMigrationVersion(db)
	if err != nil {
		return err
	}

	return self.migrator.Migrate(db, version)
}

func (self *OpenHelper) migrateFromMigrationVersion(db *sql.DB) error {

	legacySchemaExists, err := checkTableExist(db, "migration_version")
	if err != nil {
		return err
	}

	if !legacySchemaExists {
		return nil
	}

	oldMigrationLastVersion := 189
	newMigrationStartVersion := 1510262030

	var dbVersion int

	if err = db.QueryRow("SELECT version FROM migration_version").Scan(&dbVersion); err != nil {
		return err
	}

	if dbVersion != oldMigrationLastVersion {
		return fmt.Errorf("Must upgrade from db version %d (concourse 3.6.0), current db version: %d", oldMigrationLastVersion, dbVersion)
	}

	if _, err = db.Exec("DROP TABLE IF EXISTS migration_version"); err != nil {
		return err
	}

	_, err = db.Exec("CREATE TABLE IF NOT EXISTS schema_migrations (version bigint, dirty boolean)")
	if err != nil {
		return err
	}

	_, err = db.Exec("INSERT INTO schema_migrations (version, dirty) VALUES ($1, false)", newMigrationStartVersion)
	if err != nil {
		return err
	}

	return nil
}

type schemaAdapter struct {
	// from when we started using github.com/mattes/migrate (v3.7.0)
	schemaMigrationsStartVersion int
	// from when we started using voyager (v4.1.0)
	newMigrationsHistoryFirstVersion int
}

func (adapter *schemaAdapter) MigrateFromOldSchema(db *sql.DB, toVersion int) (int, error) {
	if toVersion >= adapter.newMigrationsHistoryFirstVersion {
		return adapter.newMigrationsHistoryFirstVersion, nil
	}

	oldSchemaExists, err := checkTableExist(db, "schema_migrations")
	if err != nil {
		return -1, err
	}

	if !oldSchemaExists {
		_, err := db.Exec("CREATE TABLE schema_migrations (version bigint, dirty boolean)")
		if err != nil {
			return -1, err
		}

		_, err = db.Exec("INSERT INTO schema_migrations (version, dirty) VALUES ($1, false)", toVersion)
		if err != nil {
			return -1, err
		}
	} else {
		_, err := db.Exec("UPDATE schema_migrations SET version=$1, dirty=false", toVersion)
		if err != nil {
			return -1, err
		}
	}

	return adapter.newMigrationsHistoryFirstVersion, nil
}

// this does not support migrations from v3.6.0 and prior
func (adapter schemaAdapter) MigrateToOldSchema(db *sql.DB, toVersion int) error {
	if toVersion >= adapter.newMigrationsHistoryFirstVersion {
		return nil
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

func (adapter *schemaAdapter) OldSchemaLastVersion() int {
	return adapter.schemaMigrationsStartVersion
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
