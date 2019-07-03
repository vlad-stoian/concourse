package worker

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"path"
	"strconv"
	"time"

	"code.cloudfoundry.org/garden"

	"code.cloudfoundry.org/clock"
	"code.cloudfoundry.org/lager"

	"github.com/concourse/concourse/atc"

	"github.com/concourse/concourse/atc/db"
)

//go:generate counterfeiter . WorkerProvider

const taskProcessID = "task"
const taskExitStatusPropertyName = "concourse:exit-status"

type ReturnValue struct {
	Status       int
	VolumeMounts []VolumeMount
	Err error
}

type WorkerProvider interface {
	RunningWorkers(lager.Logger) ([]Worker, error)

	FindWorkerForContainer(
		logger lager.Logger,
		teamID int,
		handle string,
	) (Worker, bool, error)

	FindWorkerForVolume(
		logger lager.Logger,
		teamID int,
		handle string,
	) (Worker, bool, error)

	FindWorkersForContainerByOwner(
		logger lager.Logger,
		owner db.ContainerOwner,
	) ([]Worker, error)

	NewGardenWorker(
		logger lager.Logger,
		tikTok clock.Clock,
		savedWorker db.Worker,
		numBuildWorkers int,
	) Worker
}

var (
	ErrNoWorkers             = errors.New("no workers")
	ErrFailedAcquirePoolLock = errors.New("failed to acquire pool lock")
)

type NoCompatibleWorkersError struct {
	Spec WorkerSpec
}

func (err NoCompatibleWorkersError) Error() string {
	return fmt.Sprintf("no workers satisfying: %s", err.Spec.Description())
}

//go:generate counterfeiter . Pool

type Pool interface {
	ClientTwo

	FindOrChooseWorker(
		lager.Logger,
		WorkerSpec,
	) (Worker, error)

	RunTaskStep(
		context.Context,
		lager.Logger,
		db.ContainerOwner,
		ContainerSpec,
		WorkerSpec,
		ContainerPlacementStrategy,
		ImageFetchingDelegate,
		db.ContainerMetadata,
		atc.VersionedResourceTypes,
		atc.TaskConfig,
		chan string,
	) (int, []VolumeMount, error)
}

type ClientTwo interface {
	FindOrChooseWorkerForContainer(
		context.Context,
		lager.Logger,
		db.ContainerOwner,
		ContainerSpec,
		WorkerSpec,
		ContainerPlacementStrategy,
	) (Worker, error)
	FindVolumeForResourceCache(logger lager.Logger, spec WorkerSpec, resourceCache db.UsedResourceCache) (Volume, bool, error)
	FindOrCreateContainer(
		context.Context,
		lager.Logger,
		ImageFetchingDelegate,
		db.ContainerOwner,
		db.ContainerMetadata,
		ContainerSpec,
		WorkerSpec,
		atc.VersionedResourceTypes,
	) (Container, error)
}

type pool struct {
	provider    WorkerProvider
	rand *rand.Rand
}

func NewPool(
	provider WorkerProvider,
) Pool {
	return &pool{
		provider:    provider,
		rand:        rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (pool *pool) allSatisfying(logger lager.Logger, spec WorkerSpec) ([]Worker, error) {
	workers, err := pool.provider.RunningWorkers(logger)
	if err != nil {
		return nil, err
	}

	if len(workers) == 0 {
		return nil, ErrNoWorkers
	}

	compatibleTeamWorkers := []Worker{}
	compatibleGeneralWorkers := []Worker{}
	for _, worker := range workers {
		compatible := worker.Satisfies(logger, spec)
		if compatible {
			if worker.IsOwnedByTeam() {
				compatibleTeamWorkers = append(compatibleTeamWorkers, worker)
			} else {
				compatibleGeneralWorkers = append(compatibleGeneralWorkers, worker)
			}
		}
	}

	if len(compatibleTeamWorkers) != 0 {
		return compatibleTeamWorkers, nil
	}

	if len(compatibleGeneralWorkers) != 0 {
		return compatibleGeneralWorkers, nil
	}

	return nil, NoCompatibleWorkersError{
		Spec: spec,
	}
}

func (pool *pool) FindOrChooseWorkerForContainer(
	ctx context.Context,
	logger lager.Logger,
	owner db.ContainerOwner,
	containerSpec ContainerSpec,
	workerSpec WorkerSpec,
	strategy ContainerPlacementStrategy,
) (Worker, error) {
	workersWithContainer, err := pool.provider.FindWorkersForContainerByOwner(
		logger.Session("find-worker"),
		owner,
	)
	if err != nil {
		return nil, err
	}

	compatibleWorkers, err := pool.allSatisfying(logger, workerSpec)
	if err != nil {
		return nil, err
	}

	var worker Worker
dance:
	for _, w := range workersWithContainer {
		for _, c := range compatibleWorkers {
			if w.Name() == c.Name() {
				worker = c
				break dance
			}
		}
	}

		if worker == nil {
			worker, err = strategy.Choose(logger, compatibleWorkers, containerSpec)
			if err != nil {
				return nil, err
			}
		}

	return worker, nil
}

func (pool *pool) FindVolumeForResourceCache(logger lager.Logger, spec WorkerSpec, resourceCache db.UsedResourceCache) (Volume, bool, error) {
	workers, err := pool.allSatisfying(logger, spec)
	if err != nil {
		return nil, false, err
	}
	for _, worker := range workers {
		volume, found, err := worker.FindVolumeForResourceCache(logger, spec, resourceCache)
		if err != nil {
			return nil, false, err
		}
		if found {
			return volume, found, nil
		}
	}
	return nil, false, nil
}

func (pool *pool) FindOrChooseWorker(
	logger lager.Logger,
	workerSpec WorkerSpec,
) (Worker, error) {
	workers, err := pool.allSatisfying(logger, workerSpec)
	if err != nil {
		return nil, err
	}

	return workers[rand.Intn(len(workers))], nil
}

func (pool *pool) FindOrCreateContainer(
	ctx context.Context,
	logger lager.Logger,
	delegate ImageFetchingDelegate,
	owner db.ContainerOwner,
	metadata db.ContainerMetadata,
	containerSpec ContainerSpec,
	workerSpec WorkerSpec,
	resourceTypes atc.VersionedResourceTypes,
) (Container, error) {
	worker, err := pool.FindOrChooseWorkerForContainer(
	ctx,
		logger,
		owner,
		containerSpec,
		workerSpec,
		NewRandomPlacementStrategy(),
	)

	if err != nil {
		return nil, err
	}

	return worker.FindOrCreateContainer(
		ctx,
		logger,
		delegate,
		owner,
		metadata,
		containerSpec,
		workerSpec,
		resourceTypes,
	)
}

func (pool *pool) RunTaskStep (
	ctx context.Context,
	logger lager.Logger,
	owner db.ContainerOwner,
	containerSpec ContainerSpec,
	workerSpec WorkerSpec,
	strategy ContainerPlacementStrategy,
	delegate ImageFetchingDelegate,
	metadata db.ContainerMetadata,
	resourceTypes atc.VersionedResourceTypes,
	config atc.TaskConfig,
	events chan string,
) (int, []VolumeMount, error) {
	chosenWorker, err := pool.FindOrChooseWorkerForContainer(
		ctx,
		logger,
		owner,
		containerSpec,
		workerSpec,
		strategy,
	)
	if err != nil {
		return -1, []VolumeMount{}, err
	}

	container, err := chosenWorker.FindOrCreateContainer(
		ctx,
		logger,
		delegate,
		owner,
		metadata,
		containerSpec,
		workerSpec,
		resourceTypes,
	)

	if err != nil {
		return -1, []VolumeMount{}, err
	}

	// container already exited
	exitStatusProp, err := container.Property(taskExitStatusPropertyName)
	if err == nil {
		logger.Info("already-exited", lager.Data{"status": exitStatusProp})

		status, err := strconv.Atoi(exitStatusProp)
		if err != nil {
			return -1, []VolumeMount{}, err
		}

		return status, container.VolumeMounts(), nil
	}

	processIO := garden.ProcessIO{
		Stdout: delegate.Stdout(),
		Stderr: delegate.Stderr(),
	}

	process, err := container.Attach(taskProcessID, processIO)
	if err == nil {
		logger.Info("already-running")
	} else {
		logger.Info("spawning")

		events <- "Starting"

		process, err = container.Run(
			garden.ProcessSpec{
				ID: taskProcessID,

				Path: config.Run.Path,
				Args: config.Run.Args,

				Dir: path.Join(metadata.WorkingDirectory, config.Run.Dir),

				// Guardian sets the default TTY window size to width: 80, height: 24,
				// which creates ANSI control sequences that do not work with other window sizes
				TTY: &garden.TTYSpec{
					WindowSize: &garden.WindowSize{Columns: 500, Rows: 500},
				},
			},
			processIO,
		)
	}
	if err != nil {
		return -1, []VolumeMount{}, err
	}

	logger.Info("attached")

	exited := make(chan struct{})
	var processStatus int
	var processErr error

	go func() {
		processStatus, processErr = process.Wait()
		close(exited)
	}()

	select {
	case <-ctx.Done():
		err = container.Stop(false)
		if err != nil {
			logger.Error("stopping-container", err)
		}

		<-exited

		return -1, container.VolumeMounts(), ctx.Err()

	case <-exited:
		if processErr != nil {
			return -1, []VolumeMount{}, processErr
		}

		err = container.SetProperty(taskExitStatusPropertyName, fmt.Sprintf("%d", processStatus))
		if err != nil {
			return -1, []VolumeMount{}, err
		}

		return processStatus, container.VolumeMounts(), nil
	}
}
