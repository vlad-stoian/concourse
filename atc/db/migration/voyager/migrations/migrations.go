package migrations

import (
	"database/sql"
	"fmt"
	"reflect"

	"github.com/concourse/concourse/atc/db/encryption"
	"github.com/concourse/concourse/atc/db/migration/voyager/runner"
)

type TestGoMigrationsRunner struct {
	*sql.DB
	encryption.Strategy
}

func NewMigrationsRunner(db *sql.DB, es encryption.Strategy) runner.MigrationsRunner {
	return &TestGoMigrationsRunner{db, es}
}

func (runner *TestGoMigrationsRunner) Run(name string) error {

	res := reflect.ValueOf(runner).MethodByName(name).Call(nil)

	ret := res[0].Interface()

	if ret != nil {
		return ret.(error)
	}

	return nil
}
