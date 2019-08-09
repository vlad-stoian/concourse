package migration_test

import (
	"database/sql"
	"github.com/concourse/concourse/atc/db/migration"
	"github.com/concourse/voyager"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("SchemaAdapter", func() {
	var (
		err           error
		db            *sql.DB
		schemaAdapter voyager.SchemaAdapter
	)

	JustBeforeEach(func() {
		db, err = sql.Open("postgres", postgresRunner.DataSourceName())
		Expect(err).NotTo(HaveOccurred())

		schemaAdapter = migration.NewSchemaAdapter(migration.OldSchemaMigrationsLastVersion, migration.NewMigrationsHistoryFirstVersion)
	})

	AfterEach(func() {
		_ = db.Close()
	})

	Context("MigrateFromOldSchema", func() {
		Context("legacy schema_migrations table exists", func() {
			It("Fails if trying to upgrade from a migration_version < 1510262030", func() {
				SetupSchemaMigrationsTableToExistAtVersion(db, 1510000000, false)

				vers, err := schemaAdapter.MigrateFromOldSchema(db, 1530000000)

				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("must migrate from db version 1510262030 or higher (Concourse v3.7.0 or higher), current db version: 1510000000"))
				Expect(vers).To(BeZero())
			})

			It("Fails if trying to upgrade from a dirty migration_version", func() {
				SetupSchemaMigrationsTableToExistAtVersion(db, migration.OldSchemaMigrationsLastVersion, true)

				vers, err := schemaAdapter.MigrateFromOldSchema(db, 1530000000)

				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("db version 1510262030 is in a dirty state"))
				Expect(vers).To(BeZero())
			})
		})

		Context("legacy schema_migrations table does not exist", func() {
			It("does nothing", func() {
				vers, err := schemaAdapter.MigrateFromOldSchema(db, 1530000000)

				Expect(err).ToNot(HaveOccurred())
				Expect(vers).To(BeZero())
			})
		})
	})

	Context("MigrateToOldSchema", func() {
		Context("legacy schema_migrations table exists", func() {
			It("inserts the correct version into the schema_migrations table", func() {
				err := schemaAdapter.MigrateToOldSchema(db, 1520000000)

				Expect(err).ToNot(HaveOccurred())
				ExpectDatabaseVersionToEqual(db, 1520000000, "schema_migrations")
			})

			It("Fails if trying to downgrade to a migration_version < 1510262030", func() {
				SetupSchemaMigrationsTableToExistAtVersion(db, 1530000000, false)

				err := schemaAdapter.MigrateToOldSchema(db, 1510000000)

				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("cannot migrate down below db version 1510262030 (Concourse v3.7.0), attempted version: 1510000000"))
			})

			It("does nothing if migrating to a version newer than 1532706545", func() {
				SetupSchemaMigrationsTableToExistAtVersion(db, 1510262030, false)
				err := schemaAdapter.MigrateToOldSchema(db, 1540000000)

				Expect(err).ToNot(HaveOccurred())
				ExpectDatabaseVersionToEqual(db, 1510262030, "schema_migrations")
			})
		})

		Context("legacy schema_migrations table does not exist", func() {
			It("creates the legacy table", func() {
				Expect(SchemaMigrationsTableExists(db)).To(BeFalse())
				err := schemaAdapter.MigrateToOldSchema(db, migration.OldSchemaMigrationsLastVersion)

				Expect(err).ToNot(HaveOccurred())
				Expect(SchemaMigrationsTableExists(db)).To(BeTrue())
				ExpectDatabaseVersionToEqual(db, migration.OldSchemaMigrationsLastVersion, "schema_migrations")
			})

			It("does nothing if migrating to a version newer than 1532706545", func() {
				Expect(SchemaMigrationsTableExists(db)).To(BeFalse())
				err := schemaAdapter.MigrateToOldSchema(db, 1540000000)

				Expect(err).ToNot(HaveOccurred())
				Expect(SchemaMigrationsTableExists(db)).To(BeFalse())
			})
		})
	})

	Context("CurrentVersion", func() {
		Context("legacy schema_migrations table does not exist", func() {
			It("returns 0", func() {
				Expect(SchemaMigrationsTableExists(db)).To(BeFalse())
				vers, err := schemaAdapter.CurrentVersion(db)
				Expect(err).ToNot(HaveOccurred())
				Expect(vers).To(Equal(0))
			})
		})
		Context("legacy schema_migrations table exists", func() {
			It("returns the version number", func() {
				SetupSchemaMigrationsTableToExistAtVersion(db, migration.OldSchemaMigrationsLastVersion, false)

				vers, err := schemaAdapter.CurrentVersion(db)
				Expect(err).ToNot(HaveOccurred())
				Expect(vers).To(Equal(migration.OldSchemaMigrationsLastVersion))
			})
		})
	})
})

func SetupSchemaMigrationsTableToExistAtVersion(db *sql.DB, version int, dirty bool) {
	_, err := db.Exec(`CREATE TABLE schema_migrations(version int, dirty bool)`)
	Expect(err).NotTo(HaveOccurred())

	_, err = db.Exec(`INSERT INTO schema_migrations(version, dirty) VALUES($1, $2)`, version, dirty)
	Expect(err).NotTo(HaveOccurred())
}

func SchemaMigrationsTableExists(dbConn *sql.DB) bool {
	var exists bool
	err := dbConn.QueryRow("SELECT EXISTS(SELECT 1 FROM information_schema.tables where table_name = 'schema_migrations')").Scan(&exists)
	Expect(err).NotTo(HaveOccurred())
	return exists
}

func ExpectDatabaseVersionToEqual(db *sql.DB, version int, table string) {
	var dbVersion int
	query := "SELECT version from " + table + " LIMIT 1"
	err := db.QueryRow(query).Scan(&dbVersion)
	Expect(err).NotTo(HaveOccurred())
	Expect(dbVersion).To(Equal(version))
}
