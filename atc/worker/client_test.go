package worker_test

import (
	"errors"
	"io"
	"io/ioutil"
	"strings"

	"code.cloudfoundry.org/lager/lagertest"
	"github.com/concourse/concourse/atc"
	"github.com/concourse/concourse/atc/db/dbfakes"
	"github.com/concourse/concourse/atc/worker"
	"github.com/concourse/concourse/atc/worker/workerfakes"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Client", func() {
	var (
		logger               *lagertest.TestLogger
		fakePool             *workerfakes.FakePool
		fakeWorkerProvider   *workerfakes.FakeWorkerProvider
		fakeArtifactProvider *dbfakes.FakeArtifactProvider
		client               worker.Client
	)

	BeforeEach(func() {
		logger = lagertest.NewTestLogger("test")
		fakePool = new(workerfakes.FakePool)
		fakeWorkerProvider = new(workerfakes.FakeWorkerProvider)
		fakeArtifactProvider = new(dbfakes.FakeArtifactProvider)

		client = worker.NewClient(fakePool, fakeWorkerProvider, fakeArtifactProvider)
	})

	Describe("FindContainer", func() {
		var (
			foundContainer worker.Container
			found          bool
			findErr        error
		)

		JustBeforeEach(func() {
			foundContainer, found, findErr = client.FindContainer(
				logger,
				4567,
				"some-handle",
			)
		})

		Context("when looking up the worker errors", func() {
			BeforeEach(func() {
				fakeWorkerProvider.FindWorkerForContainerReturns(nil, false, errors.New("nope"))
			})

			It("errors", func() {
				Expect(findErr).To(HaveOccurred())
			})
		})

		Context("when worker is not found", func() {
			BeforeEach(func() {
				fakeWorkerProvider.FindWorkerForContainerReturns(nil, false, nil)
			})

			It("returns not found", func() {
				Expect(findErr).NotTo(HaveOccurred())
				Expect(found).To(BeFalse())
			})
		})

		Context("when a worker is found with the container", func() {
			var fakeWorker *workerfakes.FakeWorker
			var fakeContainer *workerfakes.FakeContainer

			BeforeEach(func() {
				fakeWorker = new(workerfakes.FakeWorker)
				fakeWorkerProvider.FindWorkerForContainerReturns(fakeWorker, true, nil)

				fakeContainer = new(workerfakes.FakeContainer)
				fakeWorker.FindContainerByHandleReturns(fakeContainer, true, nil)
			})

			It("succeeds", func() {
				Expect(found).To(BeTrue())
				Expect(findErr).NotTo(HaveOccurred())
			})

			It("returns the created container", func() {
				Expect(foundContainer).To(Equal(fakeContainer))
			})
		})
	})

	Describe("FindVolume", func() {
		var (
			foundVolume worker.Volume
			found       bool
			findErr     error
		)

		JustBeforeEach(func() {
			foundVolume, found, findErr = client.FindVolume(
				logger,
				4567,
				"some-handle",
			)
		})

		Context("when looking up the worker errors", func() {
			BeforeEach(func() {
				fakeWorkerProvider.FindWorkerForVolumeReturns(nil, false, errors.New("nope"))
			})

			It("errors", func() {
				Expect(findErr).To(HaveOccurred())
			})
		})

		Context("when worker is not found", func() {
			BeforeEach(func() {
				fakeWorkerProvider.FindWorkerForVolumeReturns(nil, false, nil)
			})

			It("returns not found", func() {
				Expect(findErr).NotTo(HaveOccurred())
				Expect(found).To(BeFalse())
			})
		})

		Context("when a worker is found with the volume", func() {
			var fakeWorker *workerfakes.FakeWorker
			var fakeVolume *workerfakes.FakeVolume

			BeforeEach(func() {
				fakeWorker = new(workerfakes.FakeWorker)
				fakeWorkerProvider.FindWorkerForVolumeReturns(fakeWorker, true, nil)

				fakeVolume = new(workerfakes.FakeVolume)
				fakeWorker.LookupVolumeReturns(fakeVolume, true, nil)
			})

			It("succeeds", func() {
				Expect(found).To(BeTrue())
				Expect(findErr).NotTo(HaveOccurred())
			})

			It("returns the volume", func() {
				Expect(foundVolume).To(Equal(fakeVolume))
			})
		})
	})

	Describe("CreateArtifact", func() {
		It("creates an artifact", func() {
			artifact := new(dbfakes.FakeWorkerArtifact)
			fakeArtifactProvider.CreateArtifactReturns(artifact, nil)
			_, err := client.CreateArtifact(logger, "some-artifact")
			Expect(err).ToNot(HaveOccurred())

			Expect(fakeArtifactProvider.CreateArtifactCallCount()).To(Equal(1))
			nameArg := fakeArtifactProvider.CreateArtifactArgsForCall(0)
			Expect(nameArg).To(Equal("some-artifact"))
		})
	})

	Describe("Store", func() {
		var (
			artifact   atc.WorkerArtifact
			readCloser io.ReadCloser
			fakeWorker *workerfakes.FakeWorker
			fakeVolume *workerfakes.FakeVolume
		)

		BeforeEach(func() {
			artifact = atc.WorkerArtifact{
				ID:   1,
				Name: "some-name",
			}
			readCloser = ioutil.NopCloser(strings.NewReader(
				`hi there`,
			))
			fakeWorker = new(workerfakes.FakeWorker)
			fakeVolume = new(workerfakes.FakeVolume)
			fakeWorker.CreateVolumeReturns(fakeVolume, nil)
		})

		Context("volume associated with artifact does not exist", func() {
			BeforeEach(func() {
				fakeWorkerProvider.FindWorkerForArtifactReturns(nil, false, nil)
				fakePool.FindOrChooseWorkerReturns(fakeWorker, nil)
			})

			It("selects a worker and creates an associated volume on it", func() {
				err := client.Store(logger, 123, artifact, "/", readCloser)
				Expect(err).ToNot(HaveOccurred())
				Expect(fakePool.FindOrChooseWorkerCallCount()).To(Equal(1))
				Expect(fakeWorker.CreateVolumeCallCount()).To(Equal(1))
			})
		})

		It("puts data into the volume", func() {
			fakeWorkerProvider.FindWorkerForArtifactReturns(fakeWorker, true, nil)

			err := client.Store(logger, 123, artifact, "/", readCloser)
			Expect(err).ToNot(HaveOccurred())
			Expect(fakeWorkerProvider.FindWorkerForArtifactCallCount()).To(Equal(1))

			l, tID, aID := fakeWorkerProvider.FindWorkerForArtifactArgsForCall(0)
			Expect(l).To(Equal(logger))
			Expect(tID).To(Equal(123))
			Expect(aID).To(Equal(artifact.ID))

			Expect(fakeVolume.StreamInCallCount()).To(Equal(1))
		})
	})
})
