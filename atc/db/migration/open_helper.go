package migration

import (
	"code.cloudfoundry.org/lager"
	"database/sql"
	"github.com/concourse/concourse/atc/db/encryption"
	"github.com/concourse/concourse/atc/db/lock"
	"github.com/concourse/concourse/atc/db/migration/migrations"
	"github.com/concourse/voyager"
	"github.com/gobuffalo/packr"
	_ "github.com/lib/pq"
)

const (
	OldSchemaMigrationsLastVersion   = 1510262030
	NewMigrationsHistoryFirstVersion = 1532706545
)

func NewOpenHelper(driver, name string, lockFactory lock.LockFactory, strategy encryption.Strategy) *OpenHelper {
	source := &packrSource{packr.NewBox("./migrations")}
	lockID := lock.NewDatabaseMigrationLockID()
	goMigrationsRunner := migrations.NewMigrations(strategy)

	schemaAdapter := NewSchemaAdapter(
		OldSchemaMigrationsLastVersion,
		NewMigrationsHistoryFirstVersion,
	)

	migrator := voyager.NewMigrator(
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

func (self *OpenHelper) CurrentVersion(logger lager.Logger) (int, error) {
	db, err := sql.Open(self.driver, self.dataSourceName)
	if err != nil {
		return -1, err
	}

	defer db.Close()
	return self.migrator.CurrentVersion(logger, db)
}

func (self *OpenHelper) SupportedVersion(logger lager.Logger) (int, error) {
	return self.migrator.SupportedVersion(logger)
}

func (self *OpenHelper) Open(logger lager.Logger) (*sql.DB, error) {
	db, err := sql.Open(self.driver, self.dataSourceName)
	if err != nil {
		return nil, err
	}

	if err := self.migrator.Up(logger, db); err != nil {
		_ = db.Close()
		return nil, err
	}

	return db, nil
}

func (self *OpenHelper) OpenAtVersion(logger lager.Logger, version int) (*sql.DB, error) {
	db, err := sql.Open(self.driver, self.dataSourceName)
	if err != nil {
		return nil, err
	}

	if err := self.migrator.Migrate(logger, db, version); err != nil {
		_ = db.Close()
		return nil, err
	}

	return db, nil
}

func (self *OpenHelper) MigrateToVersion(logger lager.Logger, version int) error {
	db, err := sql.Open(self.driver, self.dataSourceName)
	if err != nil {
		return err
	}

	defer db.Close()

	return self.migrator.Migrate(logger, db, version)
}
