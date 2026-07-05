package thrift

import (
	"github.com/databricks/databricks-sql-go/internal/backend"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
)

// toSparkParameters maps the backend-neutral, already-stringified parameters
// onto Thrift's TSparkParameter wire type. A nil Param.Value denotes SQL NULL
// and produces a TSparkParameter whose Value is nil.
func toSparkParameters(params []backend.Param) []*cli_service.TSparkParameter {
	if len(params) == 0 {
		return nil
	}
	sparkParams := make([]*cli_service.TSparkParameter, 0, len(params))
	for i := range params {
		p := params[i]

		var sparkValue *cli_service.TSparkParameterValue
		if p.Value != nil {
			// Copy into a fresh local so &v doesn't alias the loop variable's
			// slot across iterations.
			v := *p.Value
			sparkValue = &cli_service.TSparkParameterValue{StringValue: &v}
		}

		var sparkName *string
		if p.Name != "" {
			name := p.Name
			sparkName = &name
		}

		sparkType := p.Type
		sparkParams = append(sparkParams, &cli_service.TSparkParameter{
			Name:  sparkName,
			Type:  &sparkType,
			Value: sparkValue,
		})
	}
	return sparkParams
}
