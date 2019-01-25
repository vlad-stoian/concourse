package dummy

import (
	"github.com/cloudfoundry/bosh-cli/director/template"
	"github.com/concourse/concourse/atc/creds"
)

type VariablesFactory struct {
	vars template.StaticVariables
}

func NewVariablesFactory(flags []VarFlag) *VariablesFactory {
	vars := template.StaticVariables{}
	for _, flag := range flags {
		vars[flag.Name] = flag.Value
	}

	return &VariablesFactory{
		vars: vars,
	}
}

func (factory *VariablesFactory) NewVariables(teamName string, pipelineName string) creds.Variables {
	return &Variables{
		StaticVariables: factory.vars,

		TeamName:     teamName,
		PipelineName: pipelineName,
	}
}
