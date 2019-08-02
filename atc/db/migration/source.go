package migration

import (
	"github.com/gobuffalo/packr"
)

type packrSource struct {
	packr.Box
}

func (bs *packrSource) AssetNames() []string {
	migrations := []string{}
	for _, name := range bs.Box.List() {
		if name != "migrations.go" {
			migrations = append(migrations, name)
		}
	}

	return migrations
}

func (bs *packrSource) Asset(name string) ([]byte, error) {
	return bs.Box.MustBytes(name)
}
