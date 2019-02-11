package worker

import (
	"fmt"
	"path/filepath"
	"time"

	"code.cloudfoundry.org/garden"
	"code.cloudfoundry.org/lager"
	"github.com/concourse/concourse/atc/db"
	"github.com/concourse/concourse/atc/db/lock"
)

const creatingContainerRetryDelay = 1 * time.Second

func NewContainerProvider(
	gardenClient garden.Client,
	volumeClient VolumeClient,
	dbWorker db.Worker,
	dbVolumeRepository db.VolumeRepository,
	dbTeamFactory db.TeamFactory,
	lockFactory lock.LockFactory,
) ContainerProvider {

	return &containerProvider{
		gardenClient:       gardenClient,
		volumeClient:       volumeClient,
		dbVolumeRepository: dbVolumeRepository,
		dbTeamFactory:      dbTeamFactory,
		lockFactory:        lockFactory,
		worker:             dbWorker,
	}
}

//go:generate counterfeiter . ContainerProvider

type ContainerProvider interface {
	FindCreatedContainerByHandle(
		logger lager.Logger,
		handle string,
		teamID int,
	) (Container, bool, error)

	//TODO: remove this from exposed methods?
	ConstructGardenWorkerContainer(
		logger lager.Logger,
		createdContainer db.CreatedContainer,
		gardenContainer garden.Container,
	) (Container, error)

	FindOrInitializeContainer(
		logger lager.Logger,
		owner db.ContainerOwner,
		metadata db.ContainerMetadata,
	) (db.CreatingContainer, db.CreatedContainer, error)

	CreateGardenContainer(
		containerSpec ContainerSpec,
		fetchedImage FetchedImage,
		creatingContainer db.CreatingContainer,
		bindMounts []garden.BindMount,
	) (garden.Container, error)
}

// TODO: Remove the ImageFactory from the containerProvider.
// Currently, the imageFactory is only needed to create a garden
// worker in createGardenContainer. Creating a garden worker here
// is cyclical because the garden worker contains a containerProvider.
// There is an ongoing refactor that is attempting to fix this.
type containerProvider struct {
	gardenClient       garden.Client
	volumeClient       VolumeClient
	dbVolumeRepository db.VolumeRepository
	dbTeamFactory      db.TeamFactory

	lockFactory lock.LockFactory

	worker db.Worker
}

func (provider *containerProvider) FindOrInitializeContainer(
	logger lager.Logger,
	owner db.ContainerOwner,
	metadata db.ContainerMetadata,
) (db.CreatingContainer, db.CreatedContainer, error) {

	creatingContainer, createdContainer, err := provider.worker.FindContainerOnWorker(owner)
	if err != nil {
		return nil, nil, err
	}

	// TODO: fix this
	if creatingContainer != nil {
		logger = logger.WithData(lager.Data{"container": creatingContainer.Handle()})
		logger.Debug("found-container-in-db")
		return creatingContainer, createdContainer, nil
	}

	if createdContainer != nil {
		logger = logger.WithData(lager.Data{"container": createdContainer.Handle()})
		logger.Debug("found-container-in-db")
		return creatingContainer, createdContainer, nil
	}

	if creatingContainer == nil {
		logger.Debug("creating-container-in-db")
		creatingContainer, err = provider.worker.CreateContainer(
			owner,
			metadata,
		)
		if err != nil {
			logger.Error("failed-to-create-container-in-db", err)
			return nil, nil, err
		}

		logger = logger.WithData(lager.Data{"container": creatingContainer.Handle()})
		logger.Debug("created-creating-container-in-db")
	}
	return creatingContainer, nil, nil
}

func (provider *containerProvider) CreateGardenContainer(
	containerSpec ContainerSpec,
	fetchedImage FetchedImage,
	creatingContainer db.CreatingContainer,
	bindMounts []garden.BindMount,
) (garden.Container, error) {

	gardenProperties := garden.Properties{}

	if containerSpec.User != "" {
		gardenProperties[userPropertyName] = containerSpec.User
	} else {
		gardenProperties[userPropertyName] = fetchedImage.Metadata.User
	}

	env := append(fetchedImage.Metadata.Env, containerSpec.Env...)

	if provider.worker.HTTPProxyURL() != "" {
		env = append(env, fmt.Sprintf("http_proxy=%s", provider.worker.HTTPProxyURL()))
	}

	if provider.worker.HTTPSProxyURL() != "" {
		env = append(env, fmt.Sprintf("https_proxy=%s", provider.worker.HTTPSProxyURL()))
	}

	if provider.worker.NoProxy() != "" {
		env = append(env, fmt.Sprintf("no_proxy=%s", provider.worker.NoProxy()))
	}

	return provider.gardenClient.Create(garden.ContainerSpec{
		Handle:     creatingContainer.Handle(),
		RootFSPath: fetchedImage.URL,
		Privileged: fetchedImage.Privileged,
		BindMounts: bindMounts,
		Limits:     containerSpec.Limits.ToGardenLimits(),
		Env:        env,
		Properties: gardenProperties,
	})
}

func (p *containerProvider) FindCreatedContainerByHandle(
	logger lager.Logger,
	handle string,
	teamID int,
) (Container, bool, error) {
	gardenContainer, err := p.gardenClient.Lookup(handle)
	if err != nil {
		if _, ok := err.(garden.ContainerNotFoundError); ok {
			logger.Info("container-not-found")
			return nil, false, nil
		}

		logger.Error("failed-to-lookup-on-garden", err)
		return nil, false, err
	}

	createdContainer, found, err := p.dbTeamFactory.GetByID(teamID).FindCreatedContainerByHandle(handle)
	if err != nil {
		logger.Error("failed-to-lookup-in-db", err)
		return nil, false, err
	}

	if !found {
		return nil, false, nil
	}

	createdVolumes, err := p.dbVolumeRepository.FindVolumesForContainer(createdContainer)
	if err != nil {
		return nil, false, err
	}

	container, err := newGardenWorkerContainer(
		logger,
		gardenContainer,
		createdContainer,
		createdVolumes,
		p.gardenClient,
		p.volumeClient,
		p.worker.Name(),
	)

	if err != nil {
		logger.Error("failed-to-construct-container", err)
		return nil, false, err
	}

	return container, true, nil
}

func (p *containerProvider) ConstructGardenWorkerContainer(
	logger lager.Logger,
	createdContainer db.CreatedContainer,
	gardenContainer garden.Container,
) (Container, error) {
	createdVolumes, err := p.dbVolumeRepository.FindVolumesForContainer(createdContainer)
	if err != nil {
		logger.Error("failed-to-find-container-volumes", err)
		return nil, err
	}

	return newGardenWorkerContainer(
		logger,
		gardenContainer,
		createdContainer,
		createdVolumes,
		p.gardenClient,
		p.volumeClient,
		p.worker.Name(),
	)
}

func getDestinationPathsFromInputs(inputs []InputSource) []string {
	destinationPaths := make([]string, len(inputs))

	for idx, input := range inputs {
		destinationPaths[idx] = input.DestinationPath()
	}

	return destinationPaths
}

func getDestinationPathsFromOutputs(outputs OutputPaths) []string {
	var (
		idx              = 0
		destinationPaths = make([]string, len(outputs))
	)

	for _, destinationPath := range outputs {
		destinationPaths[idx] = destinationPath
		idx++
	}

	return destinationPaths
}

func anyMountTo(path string, destinationPaths []string) bool {
	for _, destinationPath := range destinationPaths {
		if filepath.Clean(destinationPath) == filepath.Clean(path) {
			return true
		}
	}

	return false
}
