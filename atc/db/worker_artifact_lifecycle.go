package db

import (
	"database/sql"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/lib/pq"
)

//go:generate counterfeiter . ArtifactProvider

type ArtifactProvider interface {
	CreateArtifact(name string) (WorkerArtifact, error)
}

//go:generate counterfeiter . WorkerArtifactLifecycle

type WorkerArtifactLifecycle interface {
	ArtifactProvider
	RemoveExpiredArtifacts() error
}

type artifactLifecycle struct {
	conn Conn
}

func NewArtifactLifecycle(conn Conn) *artifactLifecycle {
	return &artifactLifecycle{
		conn: conn,
	}
}

func (lifecycle *artifactLifecycle) RemoveExpiredArtifacts() error {

	_, err := psql.Delete("worker_artifacts").
		Where(sq.Expr("created_at < NOW() - interval '12 hours'")).
		RunWith(lifecycle.conn).
		Exec()

	return err
}

func (lifecycle *artifactLifecycle) CreateArtifact(name string) (WorkerArtifact, error) {
	var (
		id                sql.NullInt64
		createdAt         pq.NullTime
		artifactID        int
		artifactCreatedAt time.Time
	)

	row := psql.Insert("worker_artifacts").
		Columns("name").
		Values(name).
		RunWith(lifecycle.conn).
		Suffix("RETURNING id, created_at").
		QueryRow()

	err := row.Scan(&id, &createdAt)
	if err != nil {
		return nil, err
	}

	if id.Valid {
		artifactID = int(id.Int64)
	}

	if createdAt.Valid {
		artifactCreatedAt = createdAt.Time
	}

	return &artifact{
		id:        artifactID,
		createdAt: artifactCreatedAt,
		name:      name,
	}, nil
}
