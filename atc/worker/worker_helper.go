package worker

import (
	"context"
	"fmt"
	"path/filepath"

	"code.cloudfoundry.org/garden"
	"code.cloudfoundry.org/lager"
	"github.com/concourse/concourse/atc/creds"
	"github.com/concourse/concourse/atc/db"
)

func findOrInitializeContainer(
	logger lager.Logger,
	owner db.ContainerOwner,
	metadata db.ContainerMetadata,
	worker db.Worker,
) (db.CreatingContainer, db.CreatedContainer, string, error) {

	creatingContainer, createdContainer, err := worker.FindContainerOnWorker(owner)
	if err != nil {
		return nil, nil, "", err
	}

	var foundHandle string
	switch {
	case creatingContainer != nil:
		foundHandle = creatingContainer.Handle()
	case createdContainer != nil:
		foundHandle = createdContainer.Handle()
	}

	if foundHandle != "" {
		logger = logger.WithData(lager.Data{"container": foundHandle})
		logger.Debug("found-container-in-db")
		return creatingContainer, createdContainer, foundHandle, nil
	}

	if creatingContainer == nil {
		logger.Debug("creating-container-in-db")
		creatingContainer, err = worker.CreateContainer(
			owner,
			metadata,
		)
		if err != nil {
			logger.Error("failed-to-create-container-in-db", err)
			return nil, nil, "", err
		}

		foundHandle = creatingContainer.Handle()
		logger = logger.WithData(lager.Data{"container": foundHandle})
		logger.Debug("created-creating-container-in-db")
	}
	return creatingContainer, nil, foundHandle, nil
}

func createGardenContainer(
	containerSpec ContainerSpec,
	fetchedImage FetchedImage,
	creatingContainer db.CreatingContainer,
	bindMounts []garden.BindMount,
	dbWorker db.Worker,
	gardenClient garden.Client,
) (garden.Container, error) {

	gardenProperties := garden.Properties{}

	if containerSpec.User != "" {
		gardenProperties[userPropertyName] = containerSpec.User
	} else {
		gardenProperties[userPropertyName] = fetchedImage.Metadata.User
	}

	env := append(fetchedImage.Metadata.Env, containerSpec.Env...)

	if dbWorker.HTTPProxyURL() != "" {
		env = append(env, fmt.Sprintf("http_proxy=%s", dbWorker.HTTPProxyURL()))
	}

	if dbWorker.HTTPSProxyURL() != "" {
		env = append(env, fmt.Sprintf("https_proxy=%s", dbWorker.HTTPSProxyURL()))
	}

	if dbWorker.NoProxy() != "" {
		env = append(env, fmt.Sprintf("no_proxy=%s", dbWorker.NoProxy()))
	}

	return gardenClient.Create(garden.ContainerSpec{
		Handle:     creatingContainer.Handle(),
		RootFSPath: fetchedImage.URL,
		Privileged: fetchedImage.Privileged,
		BindMounts: bindMounts,
		Limits:     containerSpec.Limits.ToGardenLimits(),
		Env:        env,
		Properties: gardenProperties,
	})
}

func findCreatedContainerByHandle(
	logger lager.Logger,
	handle string,
	teamID int,
	factory db.TeamFactory,
	gardenClient garden.Client,
	volumeRepo db.VolumeRepository,
	volumeClient VolumeClient,
	workerName string,
) (Container, bool, error) {
	gardenContainer, err := gardenClient.Lookup(handle)
	if err != nil {
		if _, ok := err.(garden.ContainerNotFoundError); ok {
			logger.Info("container-not-found")
			return nil, false, nil
		}

		logger.Error("failed-to-lookup-on-garden", err)
		return nil, false, err
	}

	createdContainer, found, err := factory.GetByID(teamID).FindCreatedContainerByHandle(handle)
	if err != nil {
		logger.Error("failed-to-lookup-in-db", err)
		return nil, false, err
	}

	if !found {
		return nil, false, nil
	}

	createdVolumes, err := volumeRepo.FindVolumesForContainer(createdContainer)
	if err != nil {
		return nil, false, err
	}

	container, err := newGardenWorkerContainer(
		logger,
		gardenContainer,
		createdContainer,
		createdVolumes,
		gardenClient,
		volumeClient,
		workerName,
	)

	if err != nil {
		logger.Error("failed-to-construct-container", err)
		return nil, false, err
	}

	return container, true, nil
}

func constructGardenWorkerContainer(
	logger lager.Logger,
	createdContainer db.CreatedContainer,
	gardenContainer garden.Container,
	repository db.VolumeRepository,
	gardenClient garden.Client,
	volumeClient VolumeClient,
	workerName string,
) (Container, error) {
	createdVolumes, err := repository.FindVolumesForContainer(createdContainer)
	if err != nil {
		logger.Error("failed-to-find-container-volumes", err)
		return nil, err
	}
	return newGardenWorkerContainer(
		logger,
		gardenContainer,
		createdContainer,
		createdVolumes,
		gardenClient,
		volumeClient,
		workerName,
	)
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
