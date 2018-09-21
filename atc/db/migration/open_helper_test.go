package migration_test

import (
	"database/sql"
	"io/ioutil"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/concourse/concourse/atc/db/encryption"
	"github.com/concourse/concourse/atc/db/lock"
	"github.com/concourse/concourse/atc/db/migration"
	"github.com/concourse/concourse/atc/db/migration/voyager"
	"github.com/concourse/concourse/atc/db/migration/voyager/voyagerfakes"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const initialSchemaVersion = 1510262030
const upgradedSchemaVersion = 1510670987

var _ = Describe("OpenHelper", func() {
	var (
		err         error
		db          *sql.DB
		lockDB      *sql.DB
		lockFactory lock.LockFactory
		strategy    encryption.Strategy
		source      *voyagerfakes.FakeSource
		openHelper  *migration.OpenHelper
	)

	JustBeforeEach(func() {
		db, err = sql.Open("postgres", postgresRunner.DataSourceName())
		Expect(err).NotTo(HaveOccurred())

		lockDB, err = sql.Open("postgres", postgresRunner.DataSourceName())
		Expect(err).NotTo(HaveOccurred())

		lockFactory = lock.NewLockFactory(lockDB)
		strategy = encryption.NewNoEncryption()
		openHelper = migration.NewOpenHelper("postgres", postgresRunner.DataSourceName(), lockFactory, strategy)

		source = new(voyagerfakes.FakeSource)
		source.AssetStub = asset
	})

	AfterEach(func() {
		_ = db.Close()
		_ = lockDB.Close()
	})

	Context("legacy migration_version table exists", func() {
		It("Fails if trying to upgrade from a migration_version < 189", func() {
			SetupMigrationVersionTableToExistAtVersion(db, 188)

			err = openHelper.MigrateToVersion(5000)

			Expect(err.Error()).To(Equal("Must upgrade from db version 189 (concourse 3.6.0), current db version: 188"))

			_, err = db.Exec("SELECT version FROM migration_version")
			Expect(err).NotTo(HaveOccurred())
		})

		It("Fails if trying to upgrade from a migration_version > 189", func() {
			SetupMigrationVersionTableToExistAtVersion(db, 190)

			err = openHelper.MigrateToVersion(5000)

			Expect(err.Error()).To(Equal("Must upgrade from db version 189 (concourse 3.6.0), current db version: 190"))

			_, err = db.Exec("SELECT version FROM migration_version")
			Expect(err).NotTo(HaveOccurred())
		})

		It("Forces schema migration version to a known first version if migration_version is 189", func() {
			SetupMigrationVersionTableToExistAtVersion(db, 189)

			SetupSchemaFromFile(db, "migrations/1510262030_initial_schema.up.sql")

			err = openHelper.MigrateToVersion(initialSchemaVersion)
			Expect(err).NotTo(HaveOccurred())

			ExpectDatabaseVersionToEqual(db, initialSchemaVersion, "schema_migrations")

			ExpectMigrationVersionTableNotToExist(db)

			ExpectToBeAbleToInsertData(db)
		})

		It("Runs migrator if migration_version table does not exist", func() {

			source.AssetNamesReturns([]string{
				"1510262030_initial_schema.up.sql",
			})
			err = openHelper.MigrateToVersion(initialSchemaVersion)
			Expect(err).NotTo(HaveOccurred())

			ExpectDatabaseVersionToEqual(db, initialSchemaVersion, "migrations_history")

			ExpectMigrationVersionTableNotToExist(db)

			ExpectToBeAbleToInsertData(db)
		})

	})

	Context("Downgrades to a version that uses the old mattes/migrate schema_migrations table", func() {
		It("Downgrades to a given version and write it to a new created schema_migrations table", func() {
			source.AssetNamesReturns([]string{
				"1510262030_initial_schema.up.sql",
				"1510670987_update_unique_constraint_for_resource_caches.up.sql",
				"1510670987_update_unique_constraint_for_resource_caches.down.sql",
			})
			migrator := voyager.NewMigratorForMigrations(db, lockFactory, strategy, source)

			err := migrator.Up()
			Expect(err).NotTo(HaveOccurred())

			currentVersion, err := migrator.CurrentVersion()
			Expect(err).NotTo(HaveOccurred())
			Expect(currentVersion).To(Equal(upgradedSchemaVersion))

			err = migrator.Migrate(initialSchemaVersion)
			Expect(err).NotTo(HaveOccurred())

			currentVersion, err = migrator.CurrentVersion()
			Expect(err).NotTo(HaveOccurred())
			Expect(currentVersion).To(Equal(initialSchemaVersion))

			ExpectDatabaseVersionToEqual(db, initialSchemaVersion, "schema_migrations")

			ExpectToBeAbleToInsertData(db)
		})

		It("Downgrades to a given version and write it to the existing schema_migrations table with dirty true", func() {

			source.AssetNamesReturns([]string{
				"1510262030_initial_schema.up.sql",
				"1510670987_update_unique_constraint_for_resource_caches.up.sql",
				"1510670987_update_unique_constraint_for_resource_caches.down.sql",
			})
			migrator := voyager.NewMigratorForMigrations(db, lockFactory, strategy, source)

			err := migrator.Up()
			Expect(err).NotTo(HaveOccurred())

			currentVersion, err := migrator.CurrentVersion()
			Expect(err).NotTo(HaveOccurred())
			Expect(currentVersion).To(Equal(upgradedSchemaVersion))

			SetupSchemaMigrationsTable(db, 8878, true)

			err = migrator.Migrate(initialSchemaVersion)
			Expect(err).NotTo(HaveOccurred())

			currentVersion, err = migrator.CurrentVersion()
			Expect(err).NotTo(HaveOccurred())
			Expect(currentVersion).To(Equal(initialSchemaVersion))

			ExpectDatabaseVersionToEqual(db, initialSchemaVersion, "schema_migrations")

			ExpectToBeAbleToInsertData(db)
		})
	})

})

func SetupMigrationVersionTableToExistAtVersion(db *sql.DB, version int) {
	_, err := db.Exec(`CREATE TABLE migration_version(version int)`)
	Expect(err).NotTo(HaveOccurred())

	_, err = db.Exec(`INSERT INTO migration_version(version) VALUES($1)`, version)
	Expect(err).NotTo(HaveOccurred())
}

func ExpectMigrationVersionTableNotToExist(dbConn *sql.DB) {
	var exists string
	err := dbConn.QueryRow("SELECT EXISTS(SELECT 1 FROM information_schema.tables where table_name = 'migration_version')").Scan(&exists)
	Expect(err).NotTo(HaveOccurred())
	Expect(exists).To(Equal("false"))
}

func ExpectDatabaseVersionToEqual(db *sql.DB, version int, table string) {
	var dbVersion int
	query := "SELECT version from " + table + " LIMIT 1"
	err := db.QueryRow(query).Scan(&dbVersion)
	Expect(err).NotTo(HaveOccurred())
	Expect(dbVersion).To(Equal(version))
}

func SetupMigrationsHistoryTableToExistAtVersion(db *sql.DB, version int) {
	_, err := db.Exec(`CREATE TABLE migrations_history(version bigint, tstamp timestamp with time zone, direction varchar, status varchar, dirty boolean)`)
	Expect(err).NotTo(HaveOccurred())

	_, err = db.Exec(`INSERT INTO migrations_history(version, tstamp, direction, status, dirty) VALUES($1, current_timestamp, 'up', 'passed', false)`, version)
	Expect(err).NotTo(HaveOccurred())
}

func SetupSchemaMigrationsTable(db *sql.DB, version int, dirty bool) {
	_, err := db.Exec("CREATE TABLE IF NOT EXISTS schema_migrations (version bigint, dirty boolean)")
	Expect(err).NotTo(HaveOccurred())
	_, err = db.Exec("INSERT INTO schema_migrations (version, dirty) VALUES ($1, $2)", version, dirty)
	Expect(err).NotTo(HaveOccurred())
}

func SetupSchemaFromFile(db *sql.DB, path string) {
	migrations, err := ioutil.ReadFile(path)
	Expect(err).NotTo(HaveOccurred())

	for _, migration := range strings.Split(string(migrations), ";") {
		_, err = db.Exec(migration)
		Expect(err).NotTo(HaveOccurred())
	}
}

func ExpectDatabaseMigrationVersionToEqual(migrator voyager.Migrator, expectedVersion int) {
	var dbVersion int
	dbVersion, err := migrator.CurrentVersion()
	Expect(err).NotTo(HaveOccurred())
	Expect(dbVersion).To(Equal(expectedVersion))
}

func ExpectToBeAbleToInsertData(dbConn *sql.DB) {
	rand.Seed(time.Now().UnixNano())

	teamID := rand.Intn(10000)
	_, err := dbConn.Exec("INSERT INTO teams(id, name) VALUES ($1, $2)", teamID, strconv.Itoa(teamID))
	Expect(err).NotTo(HaveOccurred())

	pipelineID := rand.Intn(10000)
	_, err = dbConn.Exec("INSERT INTO pipelines(id, team_id, name) VALUES ($1, $2, $3)", pipelineID, teamID, strconv.Itoa(pipelineID))
	Expect(err).NotTo(HaveOccurred())

	jobID := rand.Intn(10000)
	_, err = dbConn.Exec("INSERT INTO jobs(id, pipeline_id, name, config) VALUES ($1, $2, $3, '{}')", jobID, pipelineID, strconv.Itoa(jobID))
	Expect(err).NotTo(HaveOccurred())
}

func ExpectMigrationToHaveFailed(dbConn *sql.DB, failedVersion int, expectDirty bool) {
	var status string
	var dirty bool
	err := dbConn.QueryRow("SELECT status, dirty FROM migrations_history WHERE version=$1 ORDER BY tstamp desc LIMIT 1", failedVersion).Scan(&status, &dirty)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal("failed"))
	Expect(dirty).To(Equal(expectDirty))
}
