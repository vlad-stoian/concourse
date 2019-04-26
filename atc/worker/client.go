package worker

import (
	"io"

	"code.cloudfoundry.org/lager"
	"github.com/concourse/baggageclaim"
	"github.com/concourse/concourse/atc"

	"github.com/concourse/concourse/atc/db"
)

//go:generate counterfeiter . Client

type Client interface {
	FindContainer(logger lager.Logger, teamID int, handle string) (Container, bool, error)
	FindVolume(logger lager.Logger, teamID int, handle string) (Volume, bool, error)
	CreateArtifact(logger lager.Logger, name string) (atc.WorkerArtifact, error)
	Store(logger lager.Logger, teamID int, artifact atc.WorkerArtifact, volumePath string, data io.ReadCloser) error
}

func NewClient(pool Pool, workerProvider WorkerProvider, artifactProvider db.ArtifactProvider) *client {
	return &client{
		pool:             pool,
		workerProvider:   workerProvider,
		artifactProvider: artifactProvider,
	}
}

type client struct {
	pool             Pool
	workerProvider   WorkerProvider
	artifactProvider db.ArtifactProvider
}

func (client *client) FindContainer(logger lager.Logger, teamID int, handle string) (Container, bool, error) {
	worker, found, err := client.workerProvider.FindWorkerForContainer(
		logger.Session("find-worker"),
		teamID,
		handle,
	)
	if err != nil {
		return nil, false, err
	}

	if !found {
		return nil, false, nil
	}

	return worker.FindContainerByHandle(logger, teamID, handle)
}

func (client *client) FindVolume(logger lager.Logger, teamID int, handle string) (Volume, bool, error) {
	worker, found, err := client.workerProvider.FindWorkerForVolume(
		logger.Session("find-worker"),
		teamID,
		handle,
	)
	if err != nil {
		return nil, false, err
	}

	if !found {
		return nil, false, nil
	}

	return worker.LookupVolume(logger, handle)
}

func (client *client) CreateArtifact(logger lager.Logger, name string) (atc.WorkerArtifact, error) {
	artifact, err := client.artifactProvider.CreateArtifact(name)
	if err != nil {
		logger.Error("failed-to-create-artifact", err, lager.Data{"name": name})
		return atc.WorkerArtifact{}, err
	}
	return atc.WorkerArtifact{
		ID:        artifact.ID(),
		Name:      artifact.Name(),
		BuildID:   artifact.BuildID(),
		CreatedAt: artifact.CreatedAt().Unix(),
	}, nil
}

func (client *client) Store(logger lager.Logger, teamID int, artifact atc.WorkerArtifact, volumePath string, data io.ReadCloser) error {
	var (
		worker Worker
		err    error
		found  bool
	)

	worker, found, err = client.workerProvider.FindWorkerForArtifact(logger, teamID, artifact.ID)
	if err != nil {
		logger.Error("failed-to-find-worker-for-artifact", err, lager.Data{"artifactID": artifact.ID})
		return err
	}

	if !found {
		worker, err = client.pool.FindOrChooseWorker(logger, WorkerSpec{TeamID: teamID})
		if err != nil {
			return err
		}
	}

	spec := VolumeSpec{
		Strategy: baggageclaim.EmptyStrategy{},
	}
	volume, err := worker.CreateVolume(logger, spec, teamID, artifact.ID, db.VolumeTypeArtifact)
	if err != nil {
		return err
	}

	return volume.StreamIn(volumePath, data)
}
