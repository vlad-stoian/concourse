package migration

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/concourse/concourse/atc/db/encryption"
	"github.com/concourse/concourse/atc/db/lock"
	"github.com/concourse/concourse/atc/db/migration/migrations"
	"github.com/ddadlani/voyager"
	"github.com/gobuffalo/packr"
)

func NewOpenHelper(driver, name string, strategy encryption.Strategy) *OpenHelper {
	lockID := lock.NewDatabaseMigrationLockID()[0]

	return &OpenHelper{
		driver,
		name,
		strategy,
		&PackrSource{packr.NewBox("./migrations")},
		lockID,
	}
}

type OpenHelper struct {
	driver         string
	dataSourceName string
	strategy       encryption.Strategy
	source         voyager.Source
	lockID         int
}

func (self *OpenHelper) CurrentVersion() (int, error) {
	db, err := sql.Open(self.driver, self.dataSourceName)
	if err != nil {
		return -1, err
	}

	defer db.Close()

	runner := migrations.NewMigrationsRunner(db, self.strategy)
	return voyager.NewMigrator(db, self.lockID, self.source, runner).CurrentVersion()
}

func (self *OpenHelper) SupportedVersion() (int, error) {
	db, err := sql.Open(self.driver, self.dataSourceName)
	if err != nil {
		return -1, err
	}

	defer db.Close()

	runner := migrations.NewMigrationsRunner(db, self.strategy)
	return voyager.NewMigrator(db, self.lockID, self.source, runner).SupportedVersion()
}

func (self *OpenHelper) Open() (*sql.DB, error) {
	db, err := sql.Open(self.driver, self.dataSourceName)
	if err != nil {
		return nil, err
	}

	runner := migrations.NewMigrationsRunner(db, self.strategy)
	if err := voyager.NewMigrator(db, self.lockID, self.source, runner).Up(); err != nil {
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

	runner := migrations.NewMigrationsRunner(db, self.strategy)
	if err := voyager.NewMigrator(db, self.lockID, self.source, runner).Migrate(version); err != nil {
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

	runner := migrations.NewMigrationsRunner(db, self.strategy)
	m := voyager.NewMigrator(db, self.lockID, self.source, runner)

	err = self.migrateFromMigrationVersion(db)
	if err != nil {
		return err
	}

	return m.Migrate(version)
}

func (self *OpenHelper) migrateFromMigrationVersion(db *sql.DB) error {

	if !voyager.CheckTableExist(db, "migration_version") {
		return nil
	}

	oldMigrationLastVersion := 189
	newMigrationStartVersion := 1510262030

	var err error
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

func checkTableExist(db *sql.DB, tableName string) bool {
	var exists bool
	err := db.QueryRow("SELECT EXISTS ( SELECT 1 FROM information_schema.tables WHERE table_name=$1)", tableName).Scan(&exists)
	return err != nil || exists
}

type Adapter struct {
	oldMigrationLastVersion  int
	newMigrationStartVersion int
	oldMigrationSchemaTable  string
}

func (a *Adapter) MigrateFromOldSchema(db *sql.DB) error {
	if !checkTableExist(db, a.oldMigrationSchemaTable) {
		return nil
	}

	var isDirty = false
	var existingVersion int
	err := db.QueryRow("SELECT dirty, version FROM "+a.oldMigrationSchemaTable+" LIMIT 1").Scan(&isDirty, &existingVersion)
	if err != nil {
		return err
	}

	if isDirty {
		return errors.New("cannot begin migration. Database is in a dirty state")
	}

	return nil
}

func (a *Adapter) MigrateToOldSchema(db *sql.DB, toVersion int) error {
	// newMigrationsHistoryFirstVersion := 1532706545

	if toVersion >= a.newMigrationStartVersion {
		return nil
	}

	if !checkTableExist(db, a.oldMigrationSchemaTable) {
		_, err := db.Exec("CREATE TABLE old_schema (version bigint, dirty boolean)")
		if err != nil {
			return err
		}

		_, err = db.Exec("INSERT INTO old_schema (version, dirty) VALUES ($1, false)", toVersion)
		if err != nil {
			return err
		}
	} else {
		_, err := db.Exec("UPDATE old_schema SET version=$1, dirty=false", toVersion)
		if err != nil {
			return err
		}
	}

	return nil
}

func (a *Adapter) OldSchemaLastVersion() int {
	return a.oldMigrationLastVersion
}

func (a *Adapter) FirstVersion() int {
	return a.newMigrationStartVersion
}
