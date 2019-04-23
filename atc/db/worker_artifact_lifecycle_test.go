package db_test

import (
	"time"

	"github.com/lib/pq"

	"github.com/concourse/concourse/atc/db"

	"database/sql"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("WorkerArtifactLifecycle", func() {
	var workerArtifactLifecycle db.WorkerArtifactLifecycle

	BeforeEach(func() {
		workerArtifactLifecycle = db.NewArtifactLifecycle(dbConn)
	})

	Describe("RemoveExpiredArtifacts", func() {
		JustBeforeEach(func() {
			err := workerArtifactLifecycle.RemoveExpiredArtifacts()
			Expect(err).ToNot(HaveOccurred())
		})

		Context("removes artifacts created more than 12 hours ago", func() {

			BeforeEach(func() {
				_, err := dbConn.Exec("INSERT INTO worker_artifacts(name, created_at) VALUES('some-name', NOW() - '13 hours'::interval)")
				Expect(err).ToNot(HaveOccurred())
			})

			It("removes the record", func() {
				var count int
				err := dbConn.QueryRow("SELECT count(*) from worker_artifacts").Scan(&count)
				Expect(err).ToNot(HaveOccurred())
				Expect(count).To(Equal(0))
			})
		})

		Context("keeps artifacts for 12 hours", func() {

			BeforeEach(func() {
				_, err := dbConn.Exec("INSERT INTO worker_artifacts(name, created_at) VALUES('some-name', NOW() - '13 hours'::interval)")
				Expect(err).ToNot(HaveOccurred())

				_, err = dbConn.Exec("INSERT INTO worker_artifacts(name, created_at) VALUES('some-other-name', NOW())")
				Expect(err).ToNot(HaveOccurred())
			})

			It("does not remove the record", func() {
				var count int
				err := dbConn.QueryRow("SELECT count(*) from worker_artifacts").Scan(&count)
				Expect(err).ToNot(HaveOccurred())
				Expect(count).To(Equal(1))
			})
		})
	})

	Describe("CreateArtifact", func() {
		var (
			artifactID   sql.NullInt64
			artifactName sql.NullString
			createdAt    pq.NullTime
		)
		It("adds an artifact row to the DB", func() {
			currentTime := time.Now()
			artifact, err := workerArtifactLifecycle.CreateArtifact("some-artifact")
			Expect(err).ToNot(HaveOccurred())

			err = dbConn.QueryRow("SELECT id, name, created_at FROM worker_artifacts").Scan(&artifactID, &artifactName, &createdAt)
			Expect(err).ToNot(HaveOccurred())
			Expect(artifactID.Int64).To(BeEquivalentTo(artifact.ID()))
			Expect(artifactName.String).To(Equal(artifact.Name()))
			Expect(createdAt.Time.After(currentTime))
		})

	})
})
