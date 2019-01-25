package dummy

import "github.com/cloudfoundry/bosh-cli/director/template"

type Variables struct {
	template.StaticVariables

	TeamName     string
	PipelineName string
}

func (vars *Variables) Get(varDef template.VariableDefinition) (interface{}, bool, error) {
	pipelineDef := varDef
	pipelineDef.Name = vars.TeamName + "/" + vars.PipelineName + "/" + varDef.Name
	v, found, err := vars.StaticVariables.Get(pipelineDef)
	if err != nil {
		return nil, false, err
	}

	if found {
		return v, found, nil
	}

	teamDef := varDef
	teamDef.Name = vars.TeamName + "/" + varDef.Name
	v, found, err = vars.StaticVariables.Get(teamDef)
	if err != nil {
		return nil, false, err
	}

	if found {
		return v, found, nil
	}

	return vars.StaticVariables.Get(varDef)
}
