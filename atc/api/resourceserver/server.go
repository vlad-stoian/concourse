package resourceserver

import (
	"code.cloudfoundry.org/lager"
	"github.com/concourse/concourse/atc/creds"
	"github.com/concourse/concourse/atc/db"
)

type Server struct {
	logger                lager.Logger
	secretManager         creds.Secrets
	checkFactory          db.CheckFactory
	resourceFactory       db.ResourceFactory
	resourceConfigFactory db.ResourceConfigFactory
}

func NewServer(
	logger lager.Logger,
	secretManager creds.Secrets,
	checkFactory db.CheckFactory,
	resourceFactory db.ResourceFactory,
	resourceConfigFactory db.ResourceConfigFactory,
) *Server {
	return &Server{
		logger:                logger,
		secretManager:         secretManager,
		checkFactory:          checkFactory,
		resourceFactory:       resourceFactory,
		resourceConfigFactory: resourceConfigFactory,
	}
}
