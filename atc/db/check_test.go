package db_test

import (
	"errors"
	"time"

	"github.com/concourse/concourse/atc"
	"github.com/concourse/concourse/atc/db"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Check", func() {
	var (
		err                     error
		created                 bool
		check                   db.Check
		resourceConfigScope     db.ResourceConfigScope
		resourceTypeConfigScope db.ResourceConfigScope
	)

	BeforeEach(func() {

		setupTx, err := dbConn.Begin()
		Expect(err).ToNot(HaveOccurred())

		brt := db.BaseResourceType{
			Name: "some-base-resource-type",
		}

		_, err = brt.FindOrCreate(setupTx, false)
		Expect(err).NotTo(HaveOccurred())
		Expect(setupTx.Commit()).To(Succeed())

		resourceConfigScope, err = defaultResource.SetResourceConfig(atc.Source{"some": "repository"}, atc.VersionedResourceTypes{})
		Expect(err).NotTo(HaveOccurred())

		resourceTypeConfigScope, err = defaultResourceType.SetResourceConfig(atc.Source{"some": "type-repository"}, atc.VersionedResourceTypes{})
		Expect(err).NotTo(HaveOccurred())

		check, created, err = checkFactory.CreateCheck(
			resourceConfigScope.ID(),
			resourceConfigScope.ResourceConfig().ID(),
			resourceConfigScope.ResourceConfig().OriginBaseResourceType().ID,
			defaultTeam.ID(),
			false,
			atc.Plan{},
		)
		Expect(created).To(BeTrue())
		Expect(err).NotTo(HaveOccurred())
	})

	Describe("Start", func() {
		JustBeforeEach(func() {
			err = check.Start()
		})

		It("succeeds", func() {
			Expect(err).NotTo(HaveOccurred())
		})

		It("starts the check", func() {
			check.Reload()

			Expect(check.StartTime()).To(BeTemporally("~", time.Now(), time.Second))
		})

		It("updates resource last check start time", func() {
			defaultResource.Reload()

			Expect(defaultResource.LastCheckStartTime()).To(BeTemporally("~", time.Now(), time.Second))
		})
	})

	Describe("Finish", func() {
		JustBeforeEach(func() {
			err = check.Finish()
		})

		It("succeeds", func() {
			Expect(err).NotTo(HaveOccurred())
		})

		It("finishes the check", func() {
			check.Reload()

			Expect(check.Status()).To(Equal(db.CheckStatusSucceeded))
			Expect(check.EndTime()).To(BeTemporally("~", time.Now(), time.Second))
			Expect(check.CheckError()).To(BeNil())
		})

		It("updates resource last check end time", func() {
			defaultResource.Reload()

			Expect(defaultResource.LastCheckEndTime()).To(BeTemporally("~", time.Now(), time.Second))
		})

		It("clears out the check error", func() {
			defaultResource.Reload()

			Expect(defaultResource.CheckError()).To(BeNil())
		})
	})

	Describe("FinishWithError", func() {
		JustBeforeEach(func() {
			err = check.FinishWithError(errors.New("nope"))
		})

		It("succeeds", func() {
			Expect(err).NotTo(HaveOccurred())
		})

		It("finishes the check", func() {
			check.Reload()

			Expect(check.Status()).To(Equal(db.CheckStatusErrored))
			Expect(check.EndTime()).To(BeTemporally("~", time.Now(), time.Second))
			Expect(check.CheckError()).To(Equal(errors.New("nope")))
		})

		It("updates resource last check end time", func() {
			defaultResource.Reload()

			Expect(defaultResource.LastCheckEndTime()).To(BeTemporally("~", time.Now(), time.Second))
			Expect(defaultResource.CheckError()).To(Equal(errors.New("nope")))
		})
	})

	Describe("AllCheckables", func() {
		var checkables []db.Checkable

		Context("with resources", func() {
			JustBeforeEach(func() {
				checkables, err = check.AllCheckables()
			})

			It("succeeds", func() {
				Expect(err).NotTo(HaveOccurred())
			})

			It("includes any resources that point to the scope", func() {
				defaultResource.Reload()

				Expect(checkables).To(HaveLen(1))
				Expect(checkables[0]).To(Equal(defaultResource))
			})
		})

		Context("with resource types", func() {
			BeforeEach(func() {
				check, created, err = checkFactory.CreateCheck(
					resourceTypeConfigScope.ID(),
					resourceTypeConfigScope.ResourceConfig().ID(),
					resourceTypeConfigScope.ResourceConfig().OriginBaseResourceType().ID,
					defaultTeam.ID(),
					false,
					atc.Plan{},
				)
				Expect(created).To(BeTrue())
				Expect(err).NotTo(HaveOccurred())
			})

			JustBeforeEach(func() {
				checkables, err = check.AllCheckables()
			})

			It("succeeds", func() {
				Expect(err).NotTo(HaveOccurred())
			})

			It("includes any resource type that point to the scope", func() {
				defaultResourceType.Reload()

				Expect(checkables).To(HaveLen(1))
				Expect(checkables[0]).To(Equal(defaultResourceType))
			})
		})
	})

	Describe("SaveVersions", func() {
		JustBeforeEach(func() {
			err = check.SaveVersions([]atc.Version{{"some": "version"}})
		})

		It("succeeds", func() {
			Expect(err).NotTo(HaveOccurred())
		})

		It("saves the versions on the resource config scope", func() {
			version, found, err := resourceConfigScope.LatestVersion()
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())
			Expect(version.Version()).To(Equal(db.Version{"some": "version"}))
		})
	})
})
