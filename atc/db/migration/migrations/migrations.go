package migrations

import (
	"database/sql"
	"reflect"

	"github.com/concourse/concourse/atc/db/encryption"
)

func NewMigrations(es encryption.Strategy) *migrations {
	return &migrations{Strategy: es}
}

type migrations struct {
	*sql.DB
	encryption.Strategy
}

func (self *migrations) Run(db *sql.DB, name string) error {
	self.DB = db

	res := reflect.ValueOf(self).MethodByName(name).Call(nil)

	ret := res[0].Interface()

	if ret != nil {
		return ret.(error)
	}

	return nil
}
