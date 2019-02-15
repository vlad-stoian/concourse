package worker

import (
	"context"
	"errors"
	"github.com/concourse/baggageclaim"
	"path/filepath"
	"sort"
	"time"

	"github.com/concourse/concourse/atc/metric"

	"code.cloudfoundry.org/garden"
	"code.cloudfoundry.org/lager"
	"github.com/concourse/concourse/atc"
	"github.com/concourse/concourse/atc/creds"
	"github.com/concourse/concourse/atc/db"
	"github.com/cppforlife/go-semi-semantic/version"
)

var ErrUnsupportedResourceType = errors.New("unsupported resource type")
var ErrIncompatiblePlatform = errors.New("incompatible platform")
var ErrMismatchedTags = errors.New("mismatched tags")
var ErrTeamMismatch = errors.New("mismatched team")
var ErrNotImplemented = errors.New("Not implemented")

const userPropertyName = "user"

//go:generate counterfeiter . Worker

type Worker interface {
	Client

	BuildContainers() int

	Name() string
	ResourceTypes() []atc.WorkerResourceType
	Tags() atc.Tags
	Uptime() time.Duration
	IsOwnedByTeam() bool
	Ephemeral() bool
	IsVersionCompatible(lager.Logger, version.Version) bool

	FindVolumeForResourceCache(logger lager.Logger, resourceCache db.UsedResourceCache) (Volume, bool, error)
	FindVolumeForTaskCache(lager.Logger, int, int, string, string) (Volume, bool, error)

	CertsVolume(lager.Logger) (volume Volume, found bool, err error)
	GardenClient() garden.Client
}

type gardenWorker struct {
	gardenClient    garden.Client
	volumeClient    VolumeClient
	imageFactory    ImageFactory
	volumeRepo      db.VolumeRepository
	dbTeamFactory   db.TeamFactory
	dbWorker        db.Worker
	buildContainers int
	helper          workerHelper
}

// NewGardenWorker constructs a Worker using the gardenWorker runtime implementation and allows container and volume
// creation on a specific Garden worker.
// A Garden Worker is comprised of: db.Worker, garden Client, container provider, and a volume client
func NewGardenWorker(
	gardenClient garden.Client,
	volumeRepository db.VolumeRepository,
	volumeClient VolumeClient,
	imageFactory ImageFactory,
	dbTeamFactory db.TeamFactory,
	dbWorker db.Worker,
	numBuildContainers int,
) Worker {

	workerHelper := workerHelper{
		gardenClient:  gardenClient,
		volumeClient:  volumeClient,
		volumeRepo:    volumeRepository,
		dbTeamFactory: dbTeamFactory,
		dbWorker:      dbWorker,
	}
	return &gardenWorker{
		gardenClient:    gardenClient,
		volumeClient:    volumeClient,
		imageFactory:    imageFactory,
		volumeRepo:      volumeRepository,
		dbTeamFactory:   dbTeamFactory,
		dbWorker:        dbWorker,
		buildContainers: numBuildContainers,
		helper:          workerHelper,
	}
}

func (worker *gardenWorker) GardenClient() garden.Client {
	return worker.gardenClient
}

func (worker *gardenWorker) IsVersionCompatible(logger lager.Logger, comparedVersion version.Version) bool {
	workerVersion := worker.dbWorker.Version()
	logger = logger.Session("check-version", lager.Data{
		"want-worker-version": comparedVersion.String(),
		"have-worker-version": workerVersion,
	})

	if workerVersion == nil {
		logger.Info("empty-worker-version")
		return false
	}

	v, err := version.NewVersionFromString(*workerVersion)
	if err != nil {
		logger.Error("failed-to-parse-version", err)
		return false
	}

	switch v.Release.Compare(comparedVersion.Release) {
	case 0:
		return true
	case -1:
		return false
	default:
		if v.Release.Components[0].Compare(comparedVersion.Release.Components[0]) == 0 {
			return true
		}

		return false
	}
}

func (worker *gardenWorker) FindResourceTypeByPath(path string) (atc.WorkerResourceType, bool) {
	for _, rt := range worker.dbWorker.ResourceTypes() {
		if path == rt.Image {
			return rt, true
		}
	}

	return atc.WorkerResourceType{}, false
}

func (worker *gardenWorker) FindVolumeForResourceCache(logger lager.Logger, resourceCache db.UsedResourceCache) (Volume, bool, error) {
	return worker.volumeClient.FindVolumeForResourceCache(logger, resourceCache)
}

func (worker *gardenWorker) FindVolumeForTaskCache(logger lager.Logger, teamID int, jobID int, stepName string, path string) (Volume, bool, error) {
	return worker.volumeClient.FindVolumeForTaskCache(logger, teamID, jobID, stepName, path)
}

func (worker *gardenWorker) CertsVolume(logger lager.Logger) (Volume, bool, error) {
	return worker.volumeClient.FindOrCreateVolumeForResourceCerts(logger.Session("find-or-create"))
}

func (worker *gardenWorker) LookupVolume(logger lager.Logger, handle string) (Volume, bool, error) {
	return worker.volumeClient.LookupVolume(logger, handle)
}

// If a created container exists, a garden.Container must also exist
// so this method will find it, create the corresponding worker.Container
// and return it.
// If no created container exists, FindOrCreateContainer will go through
// the container creation flow i.e. find or create a CreatingContainer,
// create the garden.Container and then the CreatedContainer
func (worker *gardenWorker) FindOrCreateContainer(
	ctx context.Context,
	logger lager.Logger,
	delegate ImageFetchingDelegate,
	owner db.ContainerOwner,
	metadata db.ContainerMetadata,
	containerSpec ContainerSpec,
	workerSpec WorkerSpec,
	resourceTypes creds.VersionedResourceTypes,
) (Container, error) {

	var (
		gardenContainer   garden.Container
		createdContainer  db.CreatedContainer
		creatingContainer db.CreatingContainer
		containerHandle string
		err               error
	)

	creatingContainer, createdContainer, containerHandle, err = worker.helper.findOrInitializeContainer(logger, owner, metadata)
	if err != nil {
		logger.Error("failed-to-find-container-in-db", err)
		return nil, err
	}

	gardenContainer, err = worker.gardenClient.Lookup(containerHandle)
	if err != nil {
		if _, ok := err.(garden.ContainerNotFoundError); !ok {
			logger.Error("failed-to-lookup-creating-container-in-garden", err)
			return nil, err
		}
	}

	if createdContainer != nil {
		logger = logger.WithData(lager.Data{"container": containerHandle})
		logger.Debug("found-created-container-in-db")

		if gardenContainer == nil {
			return nil, garden.ContainerNotFoundError{containerHandle}
		}
		return worker.helper.constructGardenWorkerContainer(
			logger,
			createdContainer,
			gardenContainer,
		)
	}

	if gardenContainer == nil {

		fetchedImage, err := worker.fetchImageForContainer(ctx, logger, containerSpec.ImageSpec, containerSpec.TeamID, delegate, resourceTypes, creatingContainer)
		if err != nil {
			creatingContainer.Failed()
			logger.Error("failed-to-fetch-image-for-container", err)
			return nil, err
		}

		volumeMounts, err := worker.createVolumes(logger, fetchedImage.Privileged, creatingContainer, containerSpec)
		if err != nil {
			creatingContainer.Failed()
			logger.Error("failed-to-create-volume-mounts-for-container", err)
			return nil, err
		}

		bindMounts, err := worker.getBindMounts(volumeMounts, containerSpec.BindMounts)
		if err != nil {
			creatingContainer.Failed()
			logger.Error("failed-to-create-bind-mounts-for-container", err)
			return nil, err
		}

		logger.Debug("creating-garden-container")

		gardenContainer, err = worker.helper.createGardenContainer(containerSpec, fetchedImage, creatingContainer, bindMounts)
		if err != nil {
			_, failedErr := creatingContainer.Failed()
			if failedErr != nil {
				logger.Error("failed-to-mark-container-as-failed", err)
			}
			metric.FailedContainers.Inc()

			logger.Error("failed-to-create-container-in-garden", err)
			return nil, err
		}

	}

	logger.Debug("created-container-in-garden")

	metric.ContainersCreated.Inc()
	createdContainer, err = creatingContainer.Created()
	if err != nil {
		logger.Error("failed-to-mark-container-as-created", err)

		_ = worker.gardenClient.Destroy(containerHandle)

		return nil, err
	}

	logger.Debug("created-container-in-db")

	return worker.helper.constructGardenWorkerContainer(
		logger,
		createdContainer,
		gardenContainer,
	)

}

func (worker *gardenWorker) FindContainerByHandle(logger lager.Logger, teamID int, handle string) (Container, bool, error) {
	return worker.helper.findCreatedContainerByHandle(logger, handle, teamID)
}

func (worker *gardenWorker) Name() string {
	return worker.dbWorker.Name()
}

func (worker *gardenWorker) ResourceTypes() []atc.WorkerResourceType {
	return worker.dbWorker.ResourceTypes()
}

func (worker *gardenWorker) Tags() atc.Tags {
	return worker.dbWorker.Tags()
}

func (worker *gardenWorker) Ephemeral() bool {
	return worker.dbWorker.Ephemeral()
}

func (worker *gardenWorker) BuildContainers() int {
	return worker.buildContainers
}

func (worker *gardenWorker) Satisfying(logger lager.Logger, spec WorkerSpec) (Worker, error) {
	workerTeamID := worker.dbWorker.TeamID()
	workerResourceTypes := worker.dbWorker.ResourceTypes()

	if spec.TeamID != workerTeamID && workerTeamID != 0 {
		return nil, ErrTeamMismatch
	}

	if spec.ResourceType != "" {
		underlyingType := determineUnderlyingTypeName(spec.ResourceType, spec.ResourceTypes)

		matchedType := false
		for _, t := range workerResourceTypes {
			if t.Type == underlyingType {
				matchedType = true
				break
			}
		}

		if !matchedType {
			return nil, ErrUnsupportedResourceType
		}
	}

	if spec.Platform != "" {
		if spec.Platform != worker.dbWorker.Platform() {
			return nil, ErrIncompatiblePlatform
		}
	}

	if !worker.tagsMatch(spec.Tags) {
		return nil, ErrMismatchedTags
	}

	return worker, nil
}

func determineUnderlyingTypeName(typeName string, resourceTypes creds.VersionedResourceTypes) string {
	resourceTypesMap := make(map[string]creds.VersionedResourceType)
	for _, resourceType := range resourceTypes {
		resourceTypesMap[resourceType.Name] = resourceType
	}
	underlyingTypeName := typeName
	underlyingType, ok := resourceTypesMap[underlyingTypeName]
	for ok {
		underlyingTypeName = underlyingType.Type
		underlyingType, ok = resourceTypesMap[underlyingTypeName]
		delete(resourceTypesMap, underlyingTypeName)
	}
	return underlyingTypeName
}

func (worker *gardenWorker) IsOwnedByTeam() bool {
	return worker.dbWorker.TeamID() != 0
}

func (worker *gardenWorker) Uptime() time.Duration {
	return time.Since(time.Unix(worker.dbWorker.StartTime(), 0))
}

func (worker *gardenWorker) getBindMounts(volumeMounts []VolumeMount, bindMountSources []BindMountSource) ([]garden.BindMount, error) {
	bindMounts := []garden.BindMount{}

	for _, mount := range bindMountSources {
		bindMount, found, mountErr := mount.VolumeOn(worker)
		if mountErr != nil {
			return nil, mountErr
		}
		if found {
			bindMounts = append(bindMounts, bindMount)
		}
	}

	for _, mount := range volumeMounts {
		bindMounts = append(bindMounts, garden.BindMount{
			SrcPath: mount.Volume.Path(),
			DstPath: mount.MountPath,
			Mode:    garden.BindMountModeRW,
		})
	}
	return bindMounts, nil
}

func (worker *gardenWorker) createVolumes(logger lager.Logger, isPrivileged bool, creatingContainer db.CreatingContainer, spec ContainerSpec) ([]VolumeMount, error) {
	var volumeMounts []VolumeMount
	var ioVolumeMounts []VolumeMount

	scratchVolume, err := worker.volumeClient.FindOrCreateVolumeForContainer(
		logger,
		VolumeSpec{
			Strategy:   baggageclaim.EmptyStrategy{},
			Privileged: isPrivileged,
		},
		creatingContainer,
		spec.TeamID,
		"/scratch",
	)
	if err != nil {
		return nil, err
	}

	scratchMount := VolumeMount{
		Volume:    scratchVolume,
		MountPath: "/scratch",
	}

	volumeMounts = append(volumeMounts, scratchMount)

	hasSpecDirInInputs := anyMountTo(spec.Dir, getDestinationPathsFromInputs(spec.Inputs))
	hasSpecDirInOutputs := anyMountTo(spec.Dir, getDestinationPathsFromOutputs(spec.Outputs))

	if spec.Dir != "" && !hasSpecDirInOutputs && !hasSpecDirInInputs {
		workdirVolume, volumeErr := worker.volumeClient.FindOrCreateVolumeForContainer(
			logger,
			VolumeSpec{
				Strategy:   baggageclaim.EmptyStrategy{},
				Privileged: isPrivileged,
			},
			creatingContainer,
			spec.TeamID,
			spec.Dir,
		)
		if volumeErr != nil {
			return nil, volumeErr
		}

		volumeMounts = append(volumeMounts, VolumeMount{
			Volume:    workdirVolume,
			MountPath: spec.Dir,
		})
	}

	inputDestinationPaths := make(map[string]bool)

	for _, inputSource := range spec.Inputs {
		var inputVolume Volume

		localVolume, found, err := inputSource.Source().VolumeOn(logger, worker)
		if err != nil {
			return nil, err
		}

		cleanedInputPath := filepath.Clean(inputSource.DestinationPath())

		if found {
			inputVolume, err = worker.volumeClient.FindOrCreateCOWVolumeForContainer(
				logger,
				VolumeSpec{
					Strategy:   localVolume.COWStrategy(),
					Privileged: isPrivileged,
				},
				creatingContainer,
				localVolume,
				spec.TeamID,
				cleanedInputPath,
			)
			if err != nil {
				return nil, err
			}
		} else {
			inputVolume, err = worker.volumeClient.FindOrCreateVolumeForContainer(
				logger,
				VolumeSpec{
					Strategy:   baggageclaim.EmptyStrategy{},
					Privileged: isPrivileged,
				},
				creatingContainer,
				spec.TeamID,
				cleanedInputPath,
			)
			if err != nil {
				return nil, err
			}

			destData := lager.Data{
				"dest-volume": inputVolume.Handle(),
				"dest-worker": inputVolume.WorkerName(),
			}
			err = inputSource.Source().StreamTo(logger.Session("stream-to", destData), inputVolume)
			if err != nil {
				return nil, err
			}
		}

		ioVolumeMounts = append(ioVolumeMounts, VolumeMount{
			Volume:    inputVolume,
			MountPath: cleanedInputPath,
		})

		inputDestinationPaths[cleanedInputPath] = true
	}

	for _, outputPath := range spec.Outputs {
		cleanedOutputPath := filepath.Clean(outputPath)

		// reuse volume if output path is the same as input
		if inputDestinationPaths[cleanedOutputPath] {
			continue
		}

		outVolume, volumeErr := worker.volumeClient.FindOrCreateVolumeForContainer(
			logger,
			VolumeSpec{
				Strategy:   baggageclaim.EmptyStrategy{},
				Privileged: isPrivileged,
			},
			creatingContainer,
			spec.TeamID,
			cleanedOutputPath,
		)
		if volumeErr != nil {
			return nil, volumeErr
		}

		ioVolumeMounts = append(ioVolumeMounts, VolumeMount{
			Volume:    outVolume,
			MountPath: cleanedOutputPath,
		})
	}

	sort.Sort(byMountPath(ioVolumeMounts))

	volumeMounts = append(volumeMounts, ioVolumeMounts...)
	return volumeMounts, nil
}

func (worker *gardenWorker) fetchImageForContainer(
	ctx context.Context,
	logger lager.Logger,
	spec ImageSpec,
	teamID int,
	delegate ImageFetchingDelegate,
	resourceTypes creds.VersionedResourceTypes,
	creatingContainer db.CreatingContainer,
) (FetchedImage, error) {
	image, err := worker.imageFactory.GetImage(
		logger,
		worker,
		worker.volumeClient,
		spec,
		teamID,
		delegate,
		resourceTypes,
	)
	if err != nil {
		return FetchedImage{}, err
	}

	logger.Debug("fetching-image")
	return image.FetchForContainer(ctx, logger, creatingContainer)
}

func (worker *gardenWorker) tagsMatch(tags []string) bool {
	workerTags := worker.dbWorker.Tags()
	if len(workerTags) > 0 && len(tags) == 0 {
		return false
	}

insert_coin:
	for _, stag := range tags {
		for _, wtag := range workerTags {
			if stag == wtag {
				continue insert_coin
			}
		}

		return false
	}

	return true
}
