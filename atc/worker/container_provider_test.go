package worker_test

import (
	"errors"
	"fmt"

	"code.cloudfoundry.org/garden"
	"code.cloudfoundry.org/garden/gardenfakes"
	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/lager/lagertest"
	"github.com/concourse/baggageclaim"
	"github.com/concourse/baggageclaim/baggageclaimfakes"
	"github.com/concourse/concourse/atc/db"
	"github.com/concourse/concourse/atc/db/dbfakes"
	"github.com/concourse/concourse/atc/db/lock/lockfakes"
	. "github.com/concourse/concourse/atc/worker"
	"github.com/concourse/concourse/atc/worker/workerfakes"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("ContainerProvider", func() {
	var (
		logger                    *lagertest.TestLogger

		fakeCreatingContainer *dbfakes.FakeCreatingContainer
		fakeCreatedContainer  *dbfakes.FakeCreatedContainer

		fakeGardenClient       *gardenfakes.FakeClient
		fakeGardenContainer    *gardenfakes.FakeContainer
		fakeVolumeClient       *workerfakes.FakeVolumeClient
		fakeImageFactory       *workerfakes.FakeImageFactory
		fakeImage              *workerfakes.FakeImage
		fakeDBTeam             *dbfakes.FakeTeam
		fakeDBWorker           *dbfakes.FakeWorker
		fakeDBVolumeRepository *dbfakes.FakeVolumeRepository
		fakeLockFactory        *lockfakes.FakeLockFactory

		containerProvider ContainerProvider

		fakeLocalInput    *workerfakes.FakeInputSource
		fakeRemoteInput   *workerfakes.FakeInputSource
		fakeRemoteInputAS *workerfakes.FakeArtifactSource

		fakeBindMount *workerfakes.FakeBindMountSource

		fakeRemoteInputContainerVolume *workerfakes.FakeVolume
		fakeLocalVolume                *workerfakes.FakeVolume
		fakeOutputVolume               *workerfakes.FakeVolume
		fakeLocalCOWVolume             *workerfakes.FakeVolume

		stubbedVolumes map[string]*workerfakes.FakeVolume
		volumeSpecs    map[string]VolumeSpec
	)

	BeforeEach(func() {
		logger = lagertest.NewTestLogger("test")

		fakeCreatingContainer = new(dbfakes.FakeCreatingContainer)
		fakeCreatingContainer.HandleReturns("some-handle")
		fakeCreatedContainer = new(dbfakes.FakeCreatedContainer)


		fakeGardenClient = new(gardenfakes.FakeClient)
		fakeVolumeClient = new(workerfakes.FakeVolumeClient)
		fakeImageFactory = new(workerfakes.FakeImageFactory)
		fakeImage = new(workerfakes.FakeImage)
		fakeImage.FetchForContainerReturns(FetchedImage{
			Metadata: ImageMetadata{
				Env: []string{"IMAGE=ENV"},
			},
			URL: "some-image-url",
		}, nil)
		fakeImageFactory.GetImageReturns(fakeImage, nil)
		fakeLockFactory = new(lockfakes.FakeLockFactory)

		fakeDBTeamFactory := new(dbfakes.FakeTeamFactory)
		fakeDBTeam = new(dbfakes.FakeTeam)
		fakeDBTeamFactory.GetByIDReturns(fakeDBTeam)
		fakeDBVolumeRepository = new(dbfakes.FakeVolumeRepository)
		fakeGardenContainer = new(gardenfakes.FakeContainer)
		fakeGardenClient.CreateReturns(fakeGardenContainer, nil)

		fakeDBWorker = new(dbfakes.FakeWorker)
		fakeDBWorker.HTTPProxyURLReturns("http://proxy.com")
		fakeDBWorker.HTTPSProxyURLReturns("https://proxy.com")
		fakeDBWorker.NoProxyReturns("http://noproxy.com")

		containerProvider = NewContainerProvider(
			fakeGardenClient,
			fakeVolumeClient,
			fakeDBWorker,
			fakeDBVolumeRepository,
			fakeDBTeamFactory,
			fakeLockFactory,
		)

		fakeLocalInput = new(workerfakes.FakeInputSource)
		fakeLocalInput.DestinationPathReturns("/some/work-dir/local-input")
		fakeLocalInputAS := new(workerfakes.FakeArtifactSource)
		fakeLocalVolume = new(workerfakes.FakeVolume)
		fakeLocalVolume.PathReturns("/fake/local/volume")
		fakeLocalVolume.COWStrategyReturns(baggageclaim.COWStrategy{
			Parent: new(baggageclaimfakes.FakeVolume),
		})
		fakeLocalInputAS.VolumeOnReturns(fakeLocalVolume, true, nil)
		fakeLocalInput.SourceReturns(fakeLocalInputAS)

		fakeBindMount = new(workerfakes.FakeBindMountSource)
		fakeBindMount.VolumeOnReturns(garden.BindMount{
			SrcPath: "some/source",
			DstPath: "some/destination",
			Mode:    garden.BindMountModeRO,
		}, true, nil)

		fakeRemoteInput = new(workerfakes.FakeInputSource)
		fakeRemoteInput.DestinationPathReturns("/some/work-dir/remote-input")
		fakeRemoteInputAS = new(workerfakes.FakeArtifactSource)
		fakeRemoteInputAS.VolumeOnReturns(nil, false, nil)
		fakeRemoteInput.SourceReturns(fakeRemoteInputAS)

		fakeScratchVolume := new(workerfakes.FakeVolume)
		fakeScratchVolume.PathReturns("/fake/scratch/volume")

		fakeWorkdirVolume := new(workerfakes.FakeVolume)
		fakeWorkdirVolume.PathReturns("/fake/work-dir/volume")

		fakeOutputVolume = new(workerfakes.FakeVolume)
		fakeOutputVolume.PathReturns("/fake/output/volume")

		fakeLocalCOWVolume = new(workerfakes.FakeVolume)
		fakeLocalCOWVolume.PathReturns("/fake/local/cow/volume")

		fakeRemoteInputContainerVolume = new(workerfakes.FakeVolume)
		fakeRemoteInputContainerVolume.PathReturns("/fake/remote/input/container/volume")

		stubbedVolumes = map[string]*workerfakes.FakeVolume{
			"/scratch":                    fakeScratchVolume,
			"/some/work-dir":              fakeWorkdirVolume,
			"/some/work-dir/local-input":  fakeLocalCOWVolume,
			"/some/work-dir/remote-input": fakeRemoteInputContainerVolume,
			"/some/work-dir/output":       fakeOutputVolume,
		}

		volumeSpecs = map[string]VolumeSpec{}

		fakeVolumeClient.FindOrCreateCOWVolumeForContainerStub = func(logger lager.Logger, volumeSpec VolumeSpec, creatingContainer db.CreatingContainer, volume Volume, teamID int, mountPath string) (Volume, error) {
			Expect(volume).To(Equal(fakeLocalVolume))

			volume, found := stubbedVolumes[mountPath]
			if !found {
				panic("unknown container volume: " + mountPath)
			}

			volumeSpecs[mountPath] = volumeSpec

			return volume, nil
		}

		fakeVolumeClient.FindOrCreateVolumeForContainerStub = func(logger lager.Logger, volumeSpec VolumeSpec, creatingContainer db.CreatingContainer, teamID int, mountPath string) (Volume, error) {
			volume, found := stubbedVolumes[mountPath]
			if !found {
				panic("unknown container volume: " + mountPath)
			}

			volumeSpecs[mountPath] = volumeSpec

			return volume, nil
		}

	})

	Describe("FindCreatedContainerByHandle", func() {
		var (
			foundContainer Container
			findErr        error
			found          bool
		)

		JustBeforeEach(func() {
			foundContainer, found, findErr = containerProvider.FindCreatedContainerByHandle(logger, "some-container-handle", 42)
		})

		Context("when the gardenClient returns a container and no error", func() {
			var (
				fakeContainer *gardenfakes.FakeContainer
			)

			BeforeEach(func() {
				fakeContainer = new(gardenfakes.FakeContainer)
				fakeContainer.HandleReturns("provider-handle")

				fakeDBVolumeRepository.FindVolumesForContainerReturns([]db.CreatedVolume{}, nil)

				fakeDBTeam.FindCreatedContainerByHandleReturns(fakeCreatedContainer, true, nil)
				fakeGardenClient.LookupReturns(fakeContainer, nil)
			})

			It("returns the container", func() {
				Expect(findErr).NotTo(HaveOccurred())
				Expect(found).To(BeTrue())
				Expect(foundContainer.Handle()).To(Equal(fakeContainer.Handle()))
			})

			Describe("the found container", func() {
				It("can be destroyed", func() {
					err := foundContainer.Destroy()
					Expect(err).NotTo(HaveOccurred())

					By("destroying via garden")
					Expect(fakeGardenClient.DestroyCallCount()).To(Equal(1))
					Expect(fakeGardenClient.DestroyArgsForCall(0)).To(Equal("provider-handle"))
				})
			})

			Context("when the concourse:volumes property is present", func() {
				var (
					expectedHandle1Volume *workerfakes.FakeVolume
					expectedHandle2Volume *workerfakes.FakeVolume
				)

				BeforeEach(func() {
					expectedHandle1Volume = new(workerfakes.FakeVolume)
					expectedHandle2Volume = new(workerfakes.FakeVolume)

					expectedHandle1Volume.HandleReturns("handle-1")
					expectedHandle2Volume.HandleReturns("handle-2")

					expectedHandle1Volume.PathReturns("/handle-1/path")
					expectedHandle2Volume.PathReturns("/handle-2/path")

					fakeVolumeClient.LookupVolumeStub = func(logger lager.Logger, handle string) (Volume, bool, error) {
						if handle == "handle-1" {
							return expectedHandle1Volume, true, nil
						} else if handle == "handle-2" {
							return expectedHandle2Volume, true, nil
						} else {
							panic("unknown handle: " + handle)
						}
					}

					dbVolume1 := new(dbfakes.FakeCreatedVolume)
					dbVolume2 := new(dbfakes.FakeCreatedVolume)
					fakeDBVolumeRepository.FindVolumesForContainerReturns([]db.CreatedVolume{dbVolume1, dbVolume2}, nil)
					dbVolume1.HandleReturns("handle-1")
					dbVolume2.HandleReturns("handle-2")
					dbVolume1.PathReturns("/handle-1/path")
					dbVolume2.PathReturns("/handle-2/path")
				})

				Describe("VolumeMounts", func() {
					It("returns all bound volumes based on properties on the container", func() {
						Expect(findErr).NotTo(HaveOccurred())
						Expect(found).To(BeTrue())
						Expect(foundContainer.VolumeMounts()).To(ConsistOf([]VolumeMount{
							{Volume: expectedHandle1Volume, MountPath: "/handle-1/path"},
							{Volume: expectedHandle2Volume, MountPath: "/handle-2/path"},
						}))
					})

					Context("when LookupVolume returns an error", func() {
						disaster := errors.New("nope")

						BeforeEach(func() {
							fakeVolumeClient.LookupVolumeReturns(nil, false, disaster)
						})

						It("returns the error on lookup", func() {
							Expect(findErr).To(Equal(disaster))
						})
					})
				})
			})

			Context("when the user property is present", func() {
				var (
					actualSpec garden.ProcessSpec
					actualIO   garden.ProcessIO
				)

				BeforeEach(func() {
					actualSpec = garden.ProcessSpec{
						Path: "some-path",
						Args: []string{"some", "args"},
						Env:  []string{"some=env"},
						Dir:  "some-dir",
					}

					actualIO = garden.ProcessIO{}

					fakeContainer.PropertiesReturns(garden.Properties{"user": "maverick"}, nil)
				})

				JustBeforeEach(func() {
					foundContainer.Run(actualSpec, actualIO)
				})

				Describe("Run", func() {
					It("calls Run() on the garden container and injects the user", func() {
						Expect(fakeContainer.RunCallCount()).To(Equal(1))
						spec, io := fakeContainer.RunArgsForCall(0)
						Expect(spec).To(Equal(garden.ProcessSpec{
							Path: "some-path",
							Args: []string{"some", "args"},
							Env:  []string{"some=env"},
							Dir:  "some-dir",
							User: "maverick",
						}))
						Expect(io).To(Equal(garden.ProcessIO{}))
					})
				})
			})

			Context("when the user property is not present", func() {
				var (
					actualSpec garden.ProcessSpec
					actualIO   garden.ProcessIO
				)

				BeforeEach(func() {
					actualSpec = garden.ProcessSpec{
						Path: "some-path",
						Args: []string{"some", "args"},
						Env:  []string{"some=env"},
						Dir:  "some-dir",
					}

					actualIO = garden.ProcessIO{}

					fakeContainer.PropertiesReturns(garden.Properties{"user": ""}, nil)
				})

				JustBeforeEach(func() {
					foundContainer.Run(actualSpec, actualIO)
				})

				Describe("Run", func() {
					It("calls Run() on the garden container and injects the default user", func() {
						Expect(fakeContainer.RunCallCount()).To(Equal(1))
						spec, io := fakeContainer.RunArgsForCall(0)
						Expect(spec).To(Equal(garden.ProcessSpec{
							Path: "some-path",
							Args: []string{"some", "args"},
							Env:  []string{"some=env"},
							Dir:  "some-dir",
							User: "root",
						}))
						Expect(io).To(Equal(garden.ProcessIO{}))
						Expect(fakeContainer.RunCallCount()).To(Equal(1))
					})
				})
			})
		})

		Context("when the gardenClient returns garden.ContainerNotFoundError", func() {
			BeforeEach(func() {
				fakeGardenClient.LookupReturns(nil, garden.ContainerNotFoundError{Handle: "some-handle"})
			})

			It("returns false and no error", func() {
				Expect(findErr).ToNot(HaveOccurred())
				Expect(found).To(BeFalse())
			})
		})

		Context("when the gardenClient returns an error", func() {
			var expectedErr error

			BeforeEach(func() {
				expectedErr = fmt.Errorf("container not found")
				fakeGardenClient.LookupReturns(nil, expectedErr)
			})

			It("returns nil and forwards the error", func() {
				Expect(findErr).To(Equal(expectedErr))

				Expect(foundContainer).To(BeNil())
			})
		})
	})
})
