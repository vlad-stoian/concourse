package worker_test

import (
	"bytes"
	"code.cloudfoundry.org/garden"
	"code.cloudfoundry.org/lager"
	"context"
	"errors"
	"github.com/concourse/baggageclaim"
	"github.com/concourse/baggageclaim/baggageclaimfakes"
	"github.com/concourse/concourse/atc/db"
	"github.com/concourse/concourse/atc/db/lock/lockfakes"
	"io/ioutil"
	"time"

	"code.cloudfoundry.org/garden/gardenfakes"

	"code.cloudfoundry.org/lager/lagertest"
	"github.com/cloudfoundry/bosh-cli/director/template"
	"github.com/concourse/concourse/atc"
	"github.com/concourse/concourse/atc/creds"
	"github.com/concourse/concourse/atc/db/dbfakes"
	. "github.com/concourse/concourse/atc/worker"
	"github.com/concourse/concourse/atc/worker/workerfakes"
	"github.com/cppforlife/go-semi-semantic/version"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Worker", func() {
	var (
		logger                *lagertest.TestLogger
		fakeVolumeClient      *workerfakes.FakeVolumeClient
		fakeContainerProvider *workerfakes.FakeContainerProvider
		activeContainers      int
		resourceTypes         []atc.WorkerResourceType
		platform              string
		tags                  atc.Tags
		teamID                int
		ephemeral             bool
		workerName            string
		workerStartTime       int64
		gardenWorker          Worker
		workerVersion         string
		fakeGardenClient      *gardenfakes.FakeClient
		fakeImageFactory      *workerfakes.FakeImageFactory
		fakeLockFactory       *lockfakes.FakeLockFactory
		fakeImage             *workerfakes.FakeImage
	)

	BeforeEach(func() {
		logger = lagertest.NewTestLogger("test")
		fakeVolumeClient = new(workerfakes.FakeVolumeClient)
		activeContainers = 42
		resourceTypes = []atc.WorkerResourceType{
			{
				Type:    "some-resource",
				Image:   "some-resource-image",
				Version: "some-version",
			},
		}
		platform = "some-platform"
		tags = atc.Tags{"some", "tags"}
		teamID = 17
		ephemeral = true
		workerName = "some-worker"
		workerStartTime = time.Now().Unix()
		workerVersion = "1.2.3"

		fakeContainerProvider = new(workerfakes.FakeContainerProvider)
		fakeGardenClient = new(gardenfakes.FakeClient)
		fakeImageFactory = new(workerfakes.FakeImageFactory)
		fakeImage = new(workerfakes.FakeImage)
		fakeImageFactory.GetImageReturns(fakeImage, nil)
	})

	JustBeforeEach(func() {
		dbWorker := new(dbfakes.FakeWorker)
		dbWorker.ActiveContainersReturns(activeContainers)
		dbWorker.ResourceTypesReturns(resourceTypes)
		dbWorker.PlatformReturns(platform)
		dbWorker.TagsReturns(tags)
		dbWorker.EphemeralReturns(ephemeral)
		dbWorker.TeamIDReturns(teamID)
		dbWorker.NameReturns(workerName)
		dbWorker.StartTimeReturns(workerStartTime)
		dbWorker.VersionReturns(&workerVersion)

		fakeLockFactory = new(lockfakes.FakeLockFactory)
		gardenWorker = NewGardenWorker(
			fakeGardenClient,
			fakeContainerProvider,
			fakeVolumeClient,
			fakeImageFactory,
			dbWorker,
			fakeLockFactory,
			0,
		)

	})

	Describe("IsVersionCompatible", func() {
		It("is compatible when versions are the same", func() {
			requiredVersion := version.MustNewVersionFromString("1.2.3")
			Expect(
				gardenWorker.IsVersionCompatible(logger, requiredVersion),
			).To(BeTrue())
		})

		It("is not compatible when versions are different in major version", func() {
			requiredVersion := version.MustNewVersionFromString("2.2.3")
			Expect(
				gardenWorker.IsVersionCompatible(logger, requiredVersion),
			).To(BeFalse())
		})

		It("is compatible when worker minor version is newer", func() {
			requiredVersion := version.MustNewVersionFromString("1.1.3")
			Expect(
				gardenWorker.IsVersionCompatible(logger, requiredVersion),
			).To(BeTrue())
		})

		It("is not compatible when worker minor version is older", func() {
			requiredVersion := version.MustNewVersionFromString("1.3.3")
			Expect(
				gardenWorker.IsVersionCompatible(logger, requiredVersion),
			).To(BeFalse())
		})

		Context("when worker version is empty", func() {
			BeforeEach(func() {
				workerVersion = ""
			})

			It("is not compatible", func() {
				requiredVersion := version.MustNewVersionFromString("1.2.3")
				Expect(
					gardenWorker.IsVersionCompatible(logger, requiredVersion),
				).To(BeFalse())
			})
		})

		Context("when worker version does not have minor version", func() {
			BeforeEach(func() {
				workerVersion = "1"
			})

			It("is compatible when it is the same", func() {
				requiredVersion := version.MustNewVersionFromString("1")
				Expect(
					gardenWorker.IsVersionCompatible(logger, requiredVersion),
				).To(BeTrue())
			})

			It("is not compatible when it is different", func() {
				requiredVersion := version.MustNewVersionFromString("2")
				Expect(
					gardenWorker.IsVersionCompatible(logger, requiredVersion),
				).To(BeFalse())
			})

			It("is not compatible when compared version has minor vesion", func() {
				requiredVersion := version.MustNewVersionFromString("1.2")
				Expect(
					gardenWorker.IsVersionCompatible(logger, requiredVersion),
				).To(BeFalse())
			})
		})
	})

	Describe("FindCreatedContainerByHandle", func() {
		var (
			handle            string
			foundContainer    Container
			existingContainer *workerfakes.FakeContainer
			found             bool
			checkErr          error
		)

		BeforeEach(func() {
			handle = "we98lsv"
			existingContainer = new(workerfakes.FakeContainer)
			fakeContainerProvider.FindCreatedContainerByHandleReturns(existingContainer, true, nil)
		})

		JustBeforeEach(func() {
			foundContainer, found, checkErr = gardenWorker.FindContainerByHandle(logger, 42, handle)
		})

		It("calls the container provider", func() {
			Expect(fakeContainerProvider.FindCreatedContainerByHandleCallCount()).To(Equal(1))

			Expect(foundContainer).To(Equal(existingContainer))
			Expect(checkErr).ToNot(HaveOccurred())
			Expect(found).To(BeTrue())
		})

	})

	Describe("Satisfying", func() {
		var (
			spec WorkerSpec

			satisfyingWorker Worker
			satisfyingErr    error

			customTypes creds.VersionedResourceTypes
		)

		BeforeEach(func() {
			variables := template.StaticVariables{}

			customTypes = creds.NewVersionedResourceTypes(variables, atc.VersionedResourceTypes{
				{
					ResourceType: atc.ResourceType{
						Name:   "custom-type-b",
						Type:   "custom-type-a",
						Source: atc.Source{"some": "source"},
					},
					Version: atc.Version{"some": "version"},
				},
				{
					ResourceType: atc.ResourceType{
						Name:   "custom-type-a",
						Type:   "some-resource",
						Source: atc.Source{"some": "source"},
					},
					Version: atc.Version{"some": "version"},
				},
				{
					ResourceType: atc.ResourceType{
						Name:   "custom-type-c",
						Type:   "custom-type-b",
						Source: atc.Source{"some": "source"},
					},
					Version: atc.Version{"some": "version"},
				},
				{
					ResourceType: atc.ResourceType{
						Name:   "custom-type-d",
						Type:   "custom-type-b",
						Source: atc.Source{"some": "source"},
					},
					Version: atc.Version{"some": "version"},
				},
				{
					ResourceType: atc.ResourceType{
						Name:   "unknown-custom-type",
						Type:   "unknown-base-type",
						Source: atc.Source{"some": "source"},
					},
					Version: atc.Version{"some": "version"},
				},
			})

			spec = WorkerSpec{
				Tags:          []string{"some", "tags"},
				TeamID:        teamID,
				ResourceTypes: customTypes,
			}
		})

		JustBeforeEach(func() {
			satisfyingWorker, satisfyingErr = gardenWorker.Satisfying(logger, spec)
		})

		Context("when the platform is compatible", func() {
			BeforeEach(func() {
				spec.Platform = "some-platform"
			})

			Context("when no tags are specified", func() {
				BeforeEach(func() {
					spec.Tags = nil
				})

				It("returns ErrIncompatiblePlatform", func() {
					Expect(satisfyingErr).To(Equal(ErrMismatchedTags))
				})
			})

			Context("when the worker has no tags", func() {
				BeforeEach(func() {
					tags = []string{}
					spec.Tags = []string{}
				})

				It("returns the worker", func() {
					Expect(satisfyingWorker).To(Equal(gardenWorker))
				})

				It("returns no error", func() {
					Expect(satisfyingErr).NotTo(HaveOccurred())
				})
			})

			Context("when all of the requested tags are present", func() {
				BeforeEach(func() {
					spec.Tags = []string{"some", "tags"}
				})

				It("returns the worker", func() {
					Expect(satisfyingWorker).To(Equal(gardenWorker))
				})

				It("returns no error", func() {
					Expect(satisfyingErr).NotTo(HaveOccurred())
				})
			})

			Context("when some of the requested tags are present", func() {
				BeforeEach(func() {
					spec.Tags = []string{"some"}
				})

				It("returns the worker", func() {
					Expect(satisfyingWorker).To(Equal(gardenWorker))
				})

				It("returns no error", func() {
					Expect(satisfyingErr).NotTo(HaveOccurred())
				})
			})

			Context("when any of the requested tags are not present", func() {
				BeforeEach(func() {
					spec.Tags = []string{"bogus", "tags"}
				})

				It("returns ErrMismatchedTags", func() {
					Expect(satisfyingErr).To(Equal(ErrMismatchedTags))
				})
			})
		})

		Context("when the platform is incompatible", func() {
			BeforeEach(func() {
				spec.Platform = "some-bogus-platform"
			})

			It("returns ErrIncompatiblePlatform", func() {
				Expect(satisfyingErr).To(Equal(ErrIncompatiblePlatform))
			})
		})

		Context("when the resource type is supported by the worker", func() {
			BeforeEach(func() {
				spec.ResourceType = "some-resource"
			})

			Context("when all of the requested tags are present", func() {
				BeforeEach(func() {
					spec.Tags = []string{"some", "tags"}
				})

				It("returns the worker", func() {
					Expect(satisfyingWorker).To(Equal(gardenWorker))
				})

				It("returns no error", func() {
					Expect(satisfyingErr).NotTo(HaveOccurred())
				})
			})

			Context("when some of the requested tags are present", func() {
				BeforeEach(func() {
					spec.Tags = []string{"some"}
				})

				It("returns the worker", func() {
					Expect(satisfyingWorker).To(Equal(gardenWorker))
				})

				It("returns no error", func() {
					Expect(satisfyingErr).NotTo(HaveOccurred())
				})
			})

			Context("when any of the requested tags are not present", func() {
				BeforeEach(func() {
					spec.Tags = []string{"bogus", "tags"}
				})

				It("returns ErrMismatchedTags", func() {
					Expect(satisfyingErr).To(Equal(ErrMismatchedTags))
				})
			})
		})

		Context("when the resource type is a custom type supported by the worker", func() {
			BeforeEach(func() {
				spec.ResourceType = "custom-type-c"
			})

			It("returns the worker", func() {
				Expect(satisfyingWorker).To(Equal(gardenWorker))
			})

			It("returns no error", func() {
				Expect(satisfyingErr).NotTo(HaveOccurred())
			})
		})

		Context("when the resource type is a custom type that overrides one supported by the worker", func() {
			BeforeEach(func() {
				variables := template.StaticVariables{}

				customTypes = creds.NewVersionedResourceTypes(variables, atc.VersionedResourceTypes{
					{
						ResourceType: atc.ResourceType{
							Name:   "some-resource",
							Type:   "some-resource",
							Source: atc.Source{"some": "source"},
						},
						Version: atc.Version{"some": "version"},
					},
				})

				spec.ResourceType = "some-resource"
			})

			It("returns the worker", func() {
				Expect(satisfyingWorker).To(Equal(gardenWorker))
			})

			It("returns no error", func() {
				Expect(satisfyingErr).NotTo(HaveOccurred())
			})
		})

		Context("when the resource type is a custom type that results in a circular dependency", func() {
			BeforeEach(func() {
				variables := template.StaticVariables{}

				customTypes = creds.NewVersionedResourceTypes(variables, atc.VersionedResourceTypes{
					atc.VersionedResourceType{
						ResourceType: atc.ResourceType{
							Name:   "circle-a",
							Type:   "circle-b",
							Source: atc.Source{"some": "source"},
						},
						Version: atc.Version{"some": "version"},
					}, atc.VersionedResourceType{
						ResourceType: atc.ResourceType{
							Name:   "circle-b",
							Type:   "circle-c",
							Source: atc.Source{"some": "source"},
						},
						Version: atc.Version{"some": "version"},
					}, atc.VersionedResourceType{
						ResourceType: atc.ResourceType{
							Name:   "circle-c",
							Type:   "circle-a",
							Source: atc.Source{"some": "source"},
						},
						Version: atc.Version{"some": "version"},
					},
				})

				spec.ResourceType = "circle-a"
			})

			It("returns ErrUnsupportedResourceType", func() {
				Expect(satisfyingErr).To(Equal(ErrUnsupportedResourceType))
			})
		})

		Context("when the resource type is a custom type not supported by the worker", func() {
			BeforeEach(func() {
				spec.ResourceType = "unknown-custom-type"
			})

			It("returns ErrUnsupportedResourceType", func() {
				Expect(satisfyingErr).To(Equal(ErrUnsupportedResourceType))
			})
		})

		Context("when the type is not supported by the worker", func() {
			BeforeEach(func() {
				spec.ResourceType = "some-other-resource"
			})

			It("returns ErrUnsupportedResourceType", func() {
				Expect(satisfyingErr).To(Equal(ErrUnsupportedResourceType))
			})
		})

		Context("when spec specifies team", func() {
			BeforeEach(func() {
				teamID = 123
				spec.TeamID = teamID
			})

			Context("when worker belongs to same team", func() {
				It("returns the worker", func() {
					Expect(satisfyingWorker).To(Equal(gardenWorker))
				})

				It("returns no error", func() {
					Expect(satisfyingErr).NotTo(HaveOccurred())
				})
			})

			Context("when worker belongs to different team", func() {
				BeforeEach(func() {
					teamID = 777
				})

				It("returns ErrTeamMismatch", func() {
					Expect(satisfyingErr).To(Equal(ErrTeamMismatch))
				})
			})

			Context("when worker does not belong to any team", func() {
				It("returns the worker", func() {
					Expect(satisfyingWorker).To(Equal(gardenWorker))
				})

				It("returns no error", func() {
					Expect(satisfyingErr).NotTo(HaveOccurred())
				})
			})
		})

		Context("when spec does not specify a team", func() {
			Context("when worker belongs to no team", func() {
				BeforeEach(func() {
					teamID = 0
				})

				It("returns the worker", func() {
					Expect(satisfyingWorker).To(Equal(gardenWorker))
				})

				It("returns no error", func() {
					Expect(satisfyingErr).NotTo(HaveOccurred())
				})
			})

			Context("when worker belongs to any team", func() {
				BeforeEach(func() {
					teamID = 555
				})

				It("returns ErrTeamMismatch", func() {
					Expect(satisfyingErr).To(Equal(ErrTeamMismatch))
				})
			})
		})
	})

	Describe("FindOrCreateContainer", func() {
		var (
			fakeDBWorker              *dbfakes.FakeWorker
			fakeLockFactory           *lockfakes.FakeLockFactory
			fakeCreatingContainer     *dbfakes.FakeCreatingContainer
			fakeCreatedContainer      *dbfakes.FakeCreatedContainer
			fakeGardenContainer       *gardenfakes.FakeContainer
			fakeImageFetchingDelegate *workerfakes.FakeImageFetchingDelegate
			fakeBaggageclaimClient    *baggageclaimfakes.FakeClient
			fakeDBTeam                *dbfakes.FakeTeam
			fakeDBVolumeRepository    *dbfakes.FakeVolumeRepository

			containerProvider ContainerProvider

			fakeLocalInput    *workerfakes.FakeInputSource
			fakeRemoteInput   *workerfakes.FakeInputSource
			fakeRemoteInputAS *workerfakes.FakeArtifactSource

			fakeBindMount *workerfakes.FakeBindMountSource

			fakeRemoteInputContainerVolume *workerfakes.FakeVolume
			fakeLocalVolume                *workerfakes.FakeVolume
			fakeOutputVolume               *workerfakes.FakeVolume
			fakeLocalCOWVolume             *workerfakes.FakeVolume

			ctx                context.Context
			containerSpec      ContainerSpec
			workerSpec         WorkerSpec
			fakeContainerOwner *dbfakes.FakeContainerOwner
			containerMetadata  db.ContainerMetadata
			resourceTypes      creds.VersionedResourceTypes

			stubbedVolumes     map[string]*workerfakes.FakeVolume
			volumeSpecs        map[string]VolumeSpec
			credsResourceTypes creds.VersionedResourceTypes

			findOrCreateErr       error
			findOrCreateContainer Container
		)

		CertsVolumeExists := func() {
			fakeCertsVolume := new(baggageclaimfakes.FakeVolume)
			fakeBaggageclaimClient.LookupVolumeReturns(fakeCertsVolume, true, nil)
		}

		BeforeEach(func() {
			fakeCreatingContainer = new(dbfakes.FakeCreatingContainer)
			fakeCreatingContainer.HandleReturns("some-handle")
			fakeCreatedContainer = new(dbfakes.FakeCreatedContainer)

			fakeLockFactory = new(lockfakes.FakeLockFactory)
			fakeImageFetchingDelegate = new(workerfakes.FakeImageFetchingDelegate)

			fakeDBWorker = new(dbfakes.FakeWorker)
			fakeDBWorker.HTTPProxyURLReturns("http://proxy.com")
			fakeDBWorker.HTTPSProxyURLReturns("https://proxy.com")
			fakeDBWorker.NoProxyReturns("http://noproxy.com")
			fakeDBWorker.CreateContainerReturns(fakeCreatingContainer, nil)
			fakeLockFactory.AcquireReturns(new(lockfakes.FakeLock), true, nil)
			fakeBaggageclaimClient = new(baggageclaimfakes.FakeClient)

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
			ctx = context.Background()

			fakeContainerOwner = new(dbfakes.FakeContainerOwner)

			fakeImage.FetchForContainerReturns(FetchedImage{
				Metadata: ImageMetadata{
					Env: []string{"IMAGE=ENV"},
				},
				URL: "some-image-url",
			}, nil)
			containerMetadata = db.ContainerMetadata{
				StepName: "some-step",
			}

			variables := template.StaticVariables{
				"secret-image":  "super-secret-image",
				"secret-source": "super-secret-source",
			}

			cpu := uint64(1024)
			memory := uint64(1024)
			containerSpec = ContainerSpec{
				TeamID: 73410,

				ImageSpec: ImageSpec{
					ImageResource: &ImageResource{
						Type:   "registry-image",
						Source: creds.NewSource(variables, atc.Source{"some": "((secret-image))"}),
					},
				},

				User: "some-user",
				Env:  []string{"SOME=ENV"},

				Dir: "/some/work-dir",

				Inputs: []InputSource{
					fakeLocalInput,
					fakeRemoteInput,
				},

				Outputs: OutputPaths{
					"some-output": "/some/work-dir/output",
				},
				BindMounts: []BindMountSource{
					fakeBindMount,
				},
				Limits: ContainerLimits{
					CPU:    &cpu,
					Memory: &memory,
				},
			}

			credsResourceTypes = creds.NewVersionedResourceTypes(variables, atc.VersionedResourceTypes{
				{
					ResourceType: atc.ResourceType{
						Type:   "some-type",
						Source: atc.Source{"some": "((secret-source))"},
					},
					Version: atc.Version{"some": "version"},
				},
			})

			workerSpec = WorkerSpec{
				TeamID:        73410,
				ResourceType:  "registry-image",
				ResourceTypes: credsResourceTypes,
			}

			fakeDBTeamFactory := new(dbfakes.FakeTeamFactory)
			fakeDBTeam = new(dbfakes.FakeTeam)
			fakeDBTeamFactory.GetByIDReturns(fakeDBTeam)
			fakeDBVolumeRepository = new(dbfakes.FakeVolumeRepository)
			fakeGardenContainer = new(gardenfakes.FakeContainer)
			fakeGardenClient.CreateReturns(fakeGardenContainer, nil)

			containerProvider = NewContainerProvider(
				fakeGardenClient,
				fakeVolumeClient,
				fakeDBWorker,
				fakeImageFactory,
				fakeDBVolumeRepository,
				fakeDBTeamFactory,
				fakeLockFactory,
			)
		})

		JustBeforeEach(func() {
			findOrCreateContainer, findOrCreateErr = containerProvider.FindOrCreateContainer(
				ctx,
				logger,
				fakeContainerOwner,
				fakeImageFetchingDelegate,
				containerMetadata,
				containerSpec,
				workerSpec,
				resourceTypes,
				fakeImage,
			)
		})
		disasterErr := errors.New("disaster")

		Context("when container exists in database in creating state", func() {
			BeforeEach(func() {
				fakeDBWorker.FindContainerOnWorkerReturns(fakeCreatingContainer, nil, nil)
			})

			Context("when container exists in garden", func() {
				BeforeEach(func() {
					fakeGardenClient.LookupReturns(fakeGardenContainer, nil)
				})

				It("does not acquire lock", func() {
					Expect(fakeLockFactory.AcquireCallCount()).To(Equal(0))
				})

				It("marks container as created", func() {
					Expect(fakeCreatingContainer.CreatedCallCount()).To(Equal(1))
				})

				It("returns worker container", func() {
					Expect(findOrCreateContainer).ToNot(BeNil())
				})
			})

			Context("when container does not exist in garden", func() {
				BeforeEach(func() {
					fakeGardenClient.LookupReturns(nil, garden.ContainerNotFoundError{})
				})
				BeforeEach(CertsVolumeExists)

				It("acquires lock", func() {
					Expect(fakeLockFactory.AcquireCallCount()).To(Equal(1))
				})

				It("creates container in garden", func() {
					Expect(fakeGardenClient.CreateCallCount()).To(Equal(1))
				})

				It("marks container as created", func() {
					Expect(fakeCreatingContainer.CreatedCallCount()).To(Equal(1))
				})

				It("returns worker container", func() {
					Expect(findOrCreateContainer).ToNot(BeNil())
				})

				Context("when failing to create container in garden", func() {
					BeforeEach(func() {
						fakeGardenClient.CreateReturns(nil, disasterErr)
					})

					It("returns an error", func() {
						Expect(findOrCreateErr).To(Equal(disasterErr))
					})

					It("does not mark container as created", func() {
						Expect(fakeCreatingContainer.CreatedCallCount()).To(Equal(0))
					})
				})
			})

			Context("when failing to acquire the lock", func() {
				BeforeEach(func() {
					fakeLock := new(lockfakes.FakeLock)

					fakeLockFactory.AcquireReturnsOnCall(0, nil, false, nil)
					fakeLockFactory.AcquireReturnsOnCall(1, fakeLock, true, nil)
				})

				// another ATC may have created the container already
				It("rechecks for created and creating container", func() {
					Expect(fakeDBWorker.FindContainerOnWorkerCallCount()).To(Equal(2))
				})
			})
		})

		Context("when container exists in database in created state", func() {
			BeforeEach(func() {
				fakeDBWorker.FindContainerOnWorkerReturns(nil, fakeCreatedContainer, nil)
			})

			Context("when container exists in garden", func() {
				BeforeEach(func() {
					fakeGardenClient.LookupReturns(fakeGardenContainer, nil)
				})

				It("returns container", func() {
					Expect(findOrCreateContainer).ToNot(BeNil())
				})
			})

			Context("when container does not exist in garden", func() {
				var containerNotFoundErr error

				BeforeEach(func() {
					containerNotFoundErr = garden.ContainerNotFoundError{}
					fakeGardenClient.LookupReturns(nil, containerNotFoundErr)
				})

				It("returns an error", func() {
					Expect(findOrCreateErr).To(Equal(containerNotFoundErr))
				})
			})
		})

		Context("when container does not exist in database", func() {
			BeforeEach(func() {
				fakeDBWorker.FindContainerOnWorkerReturns(nil, nil, nil)
			})

			Context("when the certs volume does not exist on the worker", func() {
				BeforeEach(func() {
					fakeBaggageclaimClient.LookupVolumeReturns(nil, false, nil)
				})
				It("creates the container in garden, but does not bind mount any certs", func() {
					Expect(fakeGardenClient.CreateCallCount()).To(Equal(1))
					actualSpec := fakeGardenClient.CreateArgsForCall(0)
					Expect(actualSpec.BindMounts).ToNot(ContainElement(
						garden.BindMount{
							SrcPath: "/the/certs/volume/path",
							DstPath: "/etc/ssl/certs",
							Mode:    garden.BindMountModeRO,
						},
					))
				})
			})

			BeforeEach(func() {
				fakeCertsVolume := new(baggageclaimfakes.FakeVolume)
				fakeCertsVolume.PathReturns("/the/certs/volume/path")
				fakeBaggageclaimClient.LookupVolumeReturns(fakeCertsVolume, true, nil)
			})

			It("creates container in database", func() {
				Expect(fakeDBWorker.CreateContainerCallCount()).To(Equal(1))
			})

			It("acquires lock", func() {
				Expect(fakeLockFactory.AcquireCallCount()).To(Equal(1))
			})

			It("creates the container in garden with the input and output volumes in alphabetical order", func() {
				Expect(fakeGardenClient.CreateCallCount()).To(Equal(1))

				actualSpec := fakeGardenClient.CreateArgsForCall(0)
				Expect(actualSpec).To(Equal(garden.ContainerSpec{
					Handle:     "some-handle",
					RootFSPath: "some-image-url",
					Properties: garden.Properties{"user": "some-user"},
					BindMounts: []garden.BindMount{
						{
							SrcPath: "some/source",
							DstPath: "some/destination",
							Mode:    garden.BindMountModeRO,
						},
						{
							SrcPath: "/fake/scratch/volume",
							DstPath: "/scratch",
							Mode:    garden.BindMountModeRW,
						},
						{
							SrcPath: "/fake/work-dir/volume",
							DstPath: "/some/work-dir",
							Mode:    garden.BindMountModeRW,
						},
						{
							SrcPath: "/fake/local/cow/volume",
							DstPath: "/some/work-dir/local-input",
							Mode:    garden.BindMountModeRW,
						},
						{
							SrcPath: "/fake/output/volume",
							DstPath: "/some/work-dir/output",
							Mode:    garden.BindMountModeRW,
						},
						{
							SrcPath: "/fake/remote/input/container/volume",
							DstPath: "/some/work-dir/remote-input",
							Mode:    garden.BindMountModeRW,
						},
					},
					Limits: garden.Limits{
						CPU:    garden.CPULimits{LimitInShares: 1024},
						Memory: garden.MemoryLimits{LimitInBytes: 1024},
					},
					Env: []string{
						"IMAGE=ENV",
						"SOME=ENV",
						"http_proxy=http://proxy.com",
						"https_proxy=https://proxy.com",
						"no_proxy=http://noproxy.com",
					},
				}))
			})

			Context("when the input and output destination paths overlap", func() {
				var (
					fakeRemoteInputUnderInput    *workerfakes.FakeInputSource
					fakeRemoteInputUnderInputAS  *workerfakes.FakeArtifactSource
					fakeRemoteInputUnderOutput   *workerfakes.FakeInputSource
					fakeRemoteInputUnderOutputAS *workerfakes.FakeArtifactSource

					fakeOutputUnderInputVolume                *workerfakes.FakeVolume
					fakeOutputUnderOutputVolume               *workerfakes.FakeVolume
					fakeRemoteInputUnderInputContainerVolume  *workerfakes.FakeVolume
					fakeRemoteInputUnderOutputContainerVolume *workerfakes.FakeVolume
				)

				BeforeEach(func() {
					fakeRemoteInputUnderInput = new(workerfakes.FakeInputSource)
					fakeRemoteInputUnderInput.DestinationPathReturns("/some/work-dir/remote-input/other-input")
					fakeRemoteInputUnderInputAS = new(workerfakes.FakeArtifactSource)
					fakeRemoteInputUnderInputAS.VolumeOnReturns(nil, false, nil)
					fakeRemoteInputUnderInput.SourceReturns(fakeRemoteInputUnderInputAS)

					fakeRemoteInputUnderOutput = new(workerfakes.FakeInputSource)
					fakeRemoteInputUnderOutput.DestinationPathReturns("/some/work-dir/output/input")
					fakeRemoteInputUnderOutputAS = new(workerfakes.FakeArtifactSource)
					fakeRemoteInputUnderOutputAS.VolumeOnReturns(nil, false, nil)
					fakeRemoteInputUnderOutput.SourceReturns(fakeRemoteInputUnderOutputAS)

					fakeOutputUnderInputVolume = new(workerfakes.FakeVolume)
					fakeOutputUnderInputVolume.PathReturns("/fake/output/under/input/volume")
					fakeOutputUnderOutputVolume = new(workerfakes.FakeVolume)
					fakeOutputUnderOutputVolume.PathReturns("/fake/output/other-output/volume")

					fakeRemoteInputUnderInputContainerVolume = new(workerfakes.FakeVolume)
					fakeRemoteInputUnderInputContainerVolume.PathReturns("/fake/remote/input/other-input/container/volume")
					fakeRemoteInputUnderOutputContainerVolume = new(workerfakes.FakeVolume)
					fakeRemoteInputUnderOutputContainerVolume.PathReturns("/fake/output/input/container/volume")

					stubbedVolumes["/some/work-dir/remote-input/other-input"] = fakeRemoteInputUnderInputContainerVolume
					stubbedVolumes["/some/work-dir/output/input"] = fakeRemoteInputUnderOutputContainerVolume
					stubbedVolumes["/some/work-dir/output/other-output"] = fakeOutputUnderOutputVolume
					stubbedVolumes["/some/work-dir/local-input/output"] = fakeOutputUnderInputVolume
				})

				Context("outputs are nested under inputs", func() {
					BeforeEach(func() {
						containerSpec.Inputs = []InputSource{
							fakeLocalInput,
						}
						containerSpec.Outputs = OutputPaths{
							"some-output-under-input": "/some/work-dir/local-input/output",
						}
					})

					It("creates the container with correct bind mounts", func() {
						Expect(fakeGardenClient.CreateCallCount()).To(Equal(1))

						actualSpec := fakeGardenClient.CreateArgsForCall(0)
						Expect(actualSpec).To(Equal(garden.ContainerSpec{
							Handle:     "some-handle",
							RootFSPath: "some-image-url",
							Properties: garden.Properties{"user": "some-user"},
							BindMounts: []garden.BindMount{
								{
									SrcPath: "some/source",
									DstPath: "some/destination",
									Mode:    garden.BindMountModeRO,
								},
								{
									SrcPath: "/fake/scratch/volume",
									DstPath: "/scratch",
									Mode:    garden.BindMountModeRW,
								},
								{
									SrcPath: "/fake/work-dir/volume",
									DstPath: "/some/work-dir",
									Mode:    garden.BindMountModeRW,
								},
								{
									SrcPath: "/fake/local/cow/volume",
									DstPath: "/some/work-dir/local-input",
									Mode:    garden.BindMountModeRW,
								},
								{
									SrcPath: "/fake/output/under/input/volume",
									DstPath: "/some/work-dir/local-input/output",
									Mode:    garden.BindMountModeRW,
								},
							},
							Limits: garden.Limits{
								CPU:    garden.CPULimits{LimitInShares: 1024},
								Memory: garden.MemoryLimits{LimitInBytes: 1024},
							},
							Env: []string{
								"IMAGE=ENV",
								"SOME=ENV",
								"http_proxy=http://proxy.com",
								"https_proxy=https://proxy.com",
								"no_proxy=http://noproxy.com",
							},
						}))
					})
				})

				Context("inputs are nested under inputs", func() {
					BeforeEach(func() {
						containerSpec.Inputs = []InputSource{
							fakeRemoteInput,
							fakeRemoteInputUnderInput,
						}
						containerSpec.Outputs = OutputPaths{}
					})

					It("creates the container with correct bind mounts", func() {
						Expect(fakeGardenClient.CreateCallCount()).To(Equal(1))

						actualSpec := fakeGardenClient.CreateArgsForCall(0)
						Expect(actualSpec).To(Equal(garden.ContainerSpec{
							Handle:     "some-handle",
							RootFSPath: "some-image-url",
							Properties: garden.Properties{"user": "some-user"},
							BindMounts: []garden.BindMount{
								{
									SrcPath: "some/source",
									DstPath: "some/destination",
									Mode:    garden.BindMountModeRO,
								},
								{
									SrcPath: "/fake/scratch/volume",
									DstPath: "/scratch",
									Mode:    garden.BindMountModeRW,
								},
								{
									SrcPath: "/fake/work-dir/volume",
									DstPath: "/some/work-dir",
									Mode:    garden.BindMountModeRW,
								},
								{
									SrcPath: "/fake/remote/input/container/volume",
									DstPath: "/some/work-dir/remote-input",
									Mode:    garden.BindMountModeRW,
								},
								{
									SrcPath: "/fake/remote/input/other-input/container/volume",
									DstPath: "/some/work-dir/remote-input/other-input",
									Mode:    garden.BindMountModeRW,
								},
							},
							Limits: garden.Limits{
								CPU:    garden.CPULimits{LimitInShares: 1024},
								Memory: garden.MemoryLimits{LimitInBytes: 1024},
							},
							Env: []string{
								"IMAGE=ENV",
								"SOME=ENV",
								"http_proxy=http://proxy.com",
								"https_proxy=https://proxy.com",
								"no_proxy=http://noproxy.com",
							},
						}))
					})
				})

				Context("outputs are nested under outputs", func() {
					BeforeEach(func() {
						containerSpec.Inputs = []InputSource{}
						containerSpec.Outputs = OutputPaths{
							"some-output":              "/some/work-dir/output",
							"some-output-under-output": "/some/work-dir/output/other-output",
						}
					})

					It("creates the container with correct bind mounts", func() {
						Expect(fakeGardenClient.CreateCallCount()).To(Equal(1))

						actualSpec := fakeGardenClient.CreateArgsForCall(0)
						Expect(actualSpec).To(Equal(garden.ContainerSpec{
							Handle:     "some-handle",
							RootFSPath: "some-image-url",
							Properties: garden.Properties{"user": "some-user"},
							BindMounts: []garden.BindMount{
								{
									SrcPath: "some/source",
									DstPath: "some/destination",
									Mode:    garden.BindMountModeRO,
								},
								{
									SrcPath: "/fake/scratch/volume",
									DstPath: "/scratch",
									Mode:    garden.BindMountModeRW,
								},
								{
									SrcPath: "/fake/work-dir/volume",
									DstPath: "/some/work-dir",
									Mode:    garden.BindMountModeRW,
								},
								{
									SrcPath: "/fake/output/volume",
									DstPath: "/some/work-dir/output",
									Mode:    garden.BindMountModeRW,
								},
								{
									SrcPath: "/fake/output/other-output/volume",
									DstPath: "/some/work-dir/output/other-output",
									Mode:    garden.BindMountModeRW,
								},
							},
							Limits: garden.Limits{
								CPU:    garden.CPULimits{LimitInShares: 1024},
								Memory: garden.MemoryLimits{LimitInBytes: 1024},
							},
							Env: []string{
								"IMAGE=ENV",
								"SOME=ENV",
								"http_proxy=http://proxy.com",
								"https_proxy=https://proxy.com",
								"no_proxy=http://noproxy.com",
							},
						}))
					})
				})

				Context("inputs are nested under outputs", func() {
					BeforeEach(func() {
						containerSpec.Inputs = []InputSource{
							fakeRemoteInputUnderOutput,
						}
						containerSpec.Outputs = OutputPaths{
							"some-output": "/some/work-dir/output",
						}
					})

					It("creates the container with correct bind mounts", func() {
						Expect(fakeGardenClient.CreateCallCount()).To(Equal(1))

						actualSpec := fakeGardenClient.CreateArgsForCall(0)
						Expect(actualSpec).To(Equal(garden.ContainerSpec{
							Handle:     "some-handle",
							RootFSPath: "some-image-url",
							Properties: garden.Properties{"user": "some-user"},
							BindMounts: []garden.BindMount{
								{
									SrcPath: "some/source",
									DstPath: "some/destination",
									Mode:    garden.BindMountModeRO,
								},
								{
									SrcPath: "/fake/scratch/volume",
									DstPath: "/scratch",
									Mode:    garden.BindMountModeRW,
								},
								{
									SrcPath: "/fake/work-dir/volume",
									DstPath: "/some/work-dir",
									Mode:    garden.BindMountModeRW,
								},
								{
									SrcPath: "/fake/output/volume",
									DstPath: "/some/work-dir/output",
									Mode:    garden.BindMountModeRW,
								},
								{
									SrcPath: "/fake/output/input/container/volume",
									DstPath: "/some/work-dir/output/input",
									Mode:    garden.BindMountModeRW,
								},
							},
							Limits: garden.Limits{
								CPU:    garden.CPULimits{LimitInShares: 1024},
								Memory: garden.MemoryLimits{LimitInBytes: 1024},
							},
							Env: []string{
								"IMAGE=ENV",
								"SOME=ENV",
								"http_proxy=http://proxy.com",
								"https_proxy=https://proxy.com",
								"no_proxy=http://noproxy.com",
							},
						}))

					})
				})

				Context("input and output share the same destination path", func() {
					BeforeEach(func() {
						containerSpec.Inputs = []InputSource{
							fakeRemoteInput,
						}
						containerSpec.Outputs = OutputPaths{
							"some-output": "/some/work-dir/remote-input",
						}
					})

					It("creates the container with correct bind mounts", func() {
						Expect(fakeGardenClient.CreateCallCount()).To(Equal(1))

						actualSpec := fakeGardenClient.CreateArgsForCall(0)
						Expect(actualSpec).To(Equal(garden.ContainerSpec{
							Handle:     "some-handle",
							RootFSPath: "some-image-url",
							Properties: garden.Properties{"user": "some-user"},
							BindMounts: []garden.BindMount{
								{
									SrcPath: "some/source",
									DstPath: "some/destination",
									Mode:    garden.BindMountModeRO,
								},
								{
									SrcPath: "/fake/scratch/volume",
									DstPath: "/scratch",
									Mode:    garden.BindMountModeRW,
								},
								{
									SrcPath: "/fake/work-dir/volume",
									DstPath: "/some/work-dir",
									Mode:    garden.BindMountModeRW,
								},
								{
									SrcPath: "/fake/remote/input/container/volume",
									DstPath: "/some/work-dir/remote-input",
									Mode:    garden.BindMountModeRW,
								},
							},
							Limits: garden.Limits{
								CPU:    garden.CPULimits{LimitInShares: 1024},
								Memory: garden.MemoryLimits{LimitInBytes: 1024},
							},
							Env: []string{
								"IMAGE=ENV",
								"SOME=ENV",
								"http_proxy=http://proxy.com",
								"https_proxy=https://proxy.com",
								"no_proxy=http://noproxy.com",
							},
						}))
					})

				})
			})

			It("creates each volume unprivileged", func() {
				Expect(volumeSpecs).To(Equal(map[string]VolumeSpec{
					"/scratch":                    VolumeSpec{Strategy: baggageclaim.EmptyStrategy{}},
					"/some/work-dir":              VolumeSpec{Strategy: baggageclaim.EmptyStrategy{}},
					"/some/work-dir/output":       VolumeSpec{Strategy: baggageclaim.EmptyStrategy{}},
					"/some/work-dir/local-input":  VolumeSpec{Strategy: fakeLocalVolume.COWStrategy()},
					"/some/work-dir/remote-input": VolumeSpec{Strategy: baggageclaim.EmptyStrategy{}},
				}))
			})

			It("streams remote inputs into newly created container volumes", func() {
				Expect(fakeRemoteInputAS.StreamToCallCount()).To(Equal(1))
				_, ad := fakeRemoteInputAS.StreamToArgsForCall(0)

				err := ad.StreamIn(".", bytes.NewBufferString("some-stream"))
				Expect(err).ToNot(HaveOccurred())

				Expect(fakeRemoteInputContainerVolume.StreamInCallCount()).To(Equal(1))

				dst, from := fakeRemoteInputContainerVolume.StreamInArgsForCall(0)
				Expect(dst).To(Equal("."))
				Expect(ioutil.ReadAll(from)).To(Equal([]byte("some-stream")))
			})

			It("marks container as created", func() {
				Expect(fakeCreatingContainer.CreatedCallCount()).To(Equal(1))
			})

			Context("when the fetched image was privileged", func() {
				BeforeEach(func() {
					fakeImage.FetchForContainerReturns(FetchedImage{
						Privileged: true,
						Metadata: ImageMetadata{
							Env: []string{"IMAGE=ENV"},
						},
						URL: "some-image-url",
					}, nil)
				})

				It("creates the container privileged", func() {
					Expect(fakeGardenClient.CreateCallCount()).To(Equal(1))

					actualSpec := fakeGardenClient.CreateArgsForCall(0)
					Expect(actualSpec.Privileged).To(BeTrue())
				})

				It("creates each volume privileged", func() {
					Expect(volumeSpecs).To(Equal(map[string]VolumeSpec{
						"/scratch":                    VolumeSpec{Privileged: true, Strategy: baggageclaim.EmptyStrategy{}},
						"/some/work-dir":              VolumeSpec{Privileged: true, Strategy: baggageclaim.EmptyStrategy{}},
						"/some/work-dir/output":       VolumeSpec{Privileged: true, Strategy: baggageclaim.EmptyStrategy{}},
						"/some/work-dir/local-input":  VolumeSpec{Privileged: true, Strategy: fakeLocalVolume.COWStrategy()},
						"/some/work-dir/remote-input": VolumeSpec{Privileged: true, Strategy: baggageclaim.EmptyStrategy{}},
					}))
				})

			})

			Context("when an input has the path set to the workdir itself", func() {
				BeforeEach(func() {
					fakeLocalInput.DestinationPathReturns("/some/work-dir")
					delete(stubbedVolumes, "/some/work-dir/local-input")
					stubbedVolumes["/some/work-dir"] = fakeLocalCOWVolume
				})

				It("does not create or mount a work-dir, as we support this for backwards-compatibility", func() {
					Expect(fakeGardenClient.CreateCallCount()).To(Equal(1))

					actualSpec := fakeGardenClient.CreateArgsForCall(0)
					Expect(actualSpec.BindMounts).To(Equal([]garden.BindMount{
						{
							SrcPath: "some/source",
							DstPath: "some/destination",
							Mode:    garden.BindMountModeRO,
						},
						{
							SrcPath: "/fake/scratch/volume",
							DstPath: "/scratch",
							Mode:    garden.BindMountModeRW,
						},
						{
							SrcPath: "/fake/local/cow/volume",
							DstPath: "/some/work-dir",
							Mode:    garden.BindMountModeRW,
						},
						{
							SrcPath: "/fake/output/volume",
							DstPath: "/some/work-dir/output",
							Mode:    garden.BindMountModeRW,
						},
						{
							SrcPath: "/fake/remote/input/container/volume",
							DstPath: "/some/work-dir/remote-input",
							Mode:    garden.BindMountModeRW,
						},
					}))
				})
			})

			Context("when failing to create container in garden", func() {
				BeforeEach(func() {
					fakeGardenClient.CreateReturns(nil, disasterErr)
				})

				It("returns an error", func() {
					Expect(findOrCreateErr).To(Equal(disasterErr))
				})

				It("does not mark container as created", func() {
					Expect(fakeCreatingContainer.CreatedCallCount()).To(Equal(0))
				})

				It("marks the container as failed", func() {
					Expect(fakeCreatingContainer.FailedCallCount()).To(Equal(1))
				})
			})
		})
	})

})
