package migrations

import (
	"database/sql"
	"reflect"

	"github.com/concourse/concourse/atc/db/encryption"
	"github.com/concourse/concourse/atc/db/migration/voyager/runner"
)

type GoMigrationsRunner struct {
	*sql.DB
	encryption.Strategy
}

func NewMigrationsRunner(db *sql.DB, es encryption.Strategy) runner.MigrationsRunner {
	return &GoMigrationsRunner{db, es}
}

func (runner *GoMigrationsRunner) Run(name string) error {

	res := reflect.ValueOf(runner).MethodByName(name).Call(nil)

	ret := res[0].Interface()

	if ret != nil {
		return ret.(error)
	}

	return nil
}
