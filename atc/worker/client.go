package worker

import (
	"code.cloudfoundry.org/lager"
	"github.com/concourse/concourse/atc/db"
)

//go:generate counterfeiter . Client

type Client interface {
	FindContainer(logger lager.Logger, teamID int, handle string) (Container, bool, error)
	FindVolume(logger lager.Logger, teamID int, handle string) (Volume, bool, error)
	CreateVolume(logger lager.Logger, spec VolumeSpec, teamID int, volumeType db.VolumeType) (Volume, error)
	CreateArtifact(logger lager.Logger, name string) (db.WorkerArtifact, error)
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

func (client *client) CreateVolume(logger lager.Logger, spec VolumeSpec, teamID int, volumeType db.VolumeType) (Volume, error) {
	worker, err := client.pool.FindOrChooseWorker(logger, WorkerSpec{TeamID: teamID})
	if err != nil {
		return nil, err
	}

	artifact, err := client.CreateArtifact(logger, "dummy-should-not-exist")
	if err != nil {
		return nil, err
	}

	return worker.CreateVolume(logger, spec, teamID, artifact.ID(), volumeType)
}

func (client *client) CreateArtifact(logger lager.Logger, name string) (db.WorkerArtifact, error) {
	artifact, err := client.artifactProvider.CreateArtifact(name)
	if err != nil {
		logger.Error("failed-to-create-artifact", err, lager.Data{"name": name})
		return nil, err
	}
	return artifact, nil
}
