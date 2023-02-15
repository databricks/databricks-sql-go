package client

import (
	"context"
	"net/http"
	"strings"

	dbclient "github.com/databricks/databricks-sdk-go/client"
	sdkcfg "github.com/databricks/databricks-sdk-go/config"
	sqlexec "github.com/databricks/databricks-sdk-go/service/sql"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/pkg/errors"
)

type RestClient struct {
	cfg         *config.Config
	api         sqlexec.StatementExecutionService
	firstResult map[string]ExecutionResult
}

func InitRestClient(cfg *config.Config, httpclient *http.Client) (DatabricksClient, error) {
	c, err := dbclient.New(&sdkcfg.Config{
		Host:  cfg.Host,
		Token: cfg.AccessToken,
	})
	if err != nil {
		return nil, err
	}
	api := sqlexec.NewStatementExecution(c)
	return &RestClient{
		cfg: cfg,
		api: api,
	}, nil
}

func (rc *RestClient) OpenSession(ctx context.Context, req *OpenSessionReq) (*OpenSessionResp, error) {
	return &OpenSessionResp{SessionHandle: &RestHandle{}}, nil
}

func (rc *RestClient) CloseSession(ctx context.Context, req *CloseSessionReq) (*CloseSessionResp, error) {
	return &CloseSessionResp{
		Status: &RequestStatus{
			StatusCode: "SUCCESS",
		},
	}, nil
}

func (rc *RestClient) FetchResults(ctx context.Context, req *FetchResultsReq) (*FetchResultsResp, error) {
	if rc.firstResult != nil {
		res := rc.firstResult[req.ExecutionHandle.Id()]
		defer func() {
			rc.firstResult = nil
		}()
		return &FetchResultsResp{
			Status: &RequestStatus{
				StatusCode: "SUCCESS",
			},
			ExecutionResult: ExecutionResult{
				Schema: res.Schema,
				Result: res.Result,
			},
		}, nil
	}
	resp, err := rc.api.GetStatement(ctx, sqlexec.GetStatementRequest{
		StatementId: req.ExecutionHandle.Id(),
	})
	if err != nil {
		return nil, err
	}
	return &FetchResultsResp{
		Status: &RequestStatus{
			StatusCode: "SUCCESS",
		},
		ExecutionResult: ExecutionResult{
			Schema: toSchemaRest(resp.Manifest),
			Result: toResultRest(resp.Result, false),
		},
	}, nil
}

func (rc *RestClient) GetResultsMetadata(ctx context.Context, req *GetResultsMetadataReq) (*GetResultsMetadataResp, error) {
	return nil, errors.New("get results metadata is not available in REST API")
}

func (rc *RestClient) ExecuteStatement(ctx context.Context, req *ExecuteStatementReq) (*ExecuteStatementResp, error) {

	resp, err := rc.api.ExecuteStatement(ctx, sqlexec.ExecuteStatementRequest{
		Catalog:       rc.cfg.Catalog,
		Schema:        rc.cfg.Schema,
		Disposition:   sqlexec.DispositionExternalLinks,
		Format:        sqlexec.FormatArrowStream,
		OnWaitTimeout: sqlexec.TimeoutActionContinue,
		WaitTimeout:   "5s",
		WarehouseId:   strings.TrimPrefix(rc.cfg.HTTPPath, "/sql/1.0/warehouses/"),
		Statement:     req.Statement,
	})
	if err != nil {
		return nil, err
	}

	var executionError *ExecutionError
	if resp.Status.Error != nil {
		executionError = &ExecutionError{
			// Message:   resp.Status.Error.Message,
			ErrorCode: resp.Status.Error.ErrorCode.String(),
		}
	}
	return &ExecuteStatementResp{
		ExecutionHandle: &RestHandle{
			StatementId: resp.StatementId,
		},
		ExecutionStatus: ExecutionStatus{
			ExecutionState: string(resp.Status.State),
			Error:          executionError,
		},
		ExecutionResult: ExecutionResult{
			Schema: toSchemaRest(resp.Manifest),
			Result: toResultRest(resp.Result, false),
		},
	}, nil
}

func toResultRest(r *sqlexec.ResultData, hasMoreRows bool) *ResultData {
	if r == nil {
		return nil
	}

	var resultLinks []*ResultLink

	ls := r.ExternalLinks
	for i := range ls {
		rl := &ResultLink{
			FileLink:       ls[i].ExternalLink,
			StartRowOffset: ls[i].RowOffset,
			RowCount:       ls[i].RowCount,
		}
		resultLinks = append(resultLinks, rl)
	}

	return &ResultData{
		StartRowOffset: r.RowOffset,
		ResultLinks:    resultLinks,
		HasMoreRows:    hasMoreRows,
	}
}

func (rc *RestClient) GetExecutionStatus(ctx context.Context, req *GetExecutionStatusReq) (*GetExecutionStatusResp, error) {
	resp, err := rc.api.GetStatement(ctx, sqlexec.GetStatementRequest{
		StatementId: req.ExecutionHandle.Id(),
	})
	if err != nil {
		return nil, err
	}
	if resp.Result != nil {
		rc.firstResult[req.ExecutionHandle.Id()] = ExecutionResult{
			Result: toResultRest(resp.Result, false),
			Schema: toSchemaRest(resp.Manifest),
		}
	}
	// TODO
	return &GetExecutionStatusResp{
		Status: &RequestStatus{
			StatusCode: "SUCCESS",
		},
		ExecutionStatus: ExecutionStatus{
			ExecutionState: string(resp.Status.State),
			Error: &ExecutionError{
				Message:   resp.Status.Error.Message,
				ErrorCode: resp.Status.Error.ErrorCode.String(),
			},
		},
	}, nil
}

func (rc *RestClient) CloseExecution(ctx context.Context, req *CloseExecutionReq) (*CloseExecutionResp, error) {
	// not implemented
	return &CloseExecutionResp{
		Status: &RequestStatus{
			StatusCode: "SUCCESS",
		},
	}, nil
}

func (rc *RestClient) CancelExecution(ctx context.Context, req *CancelExecutionReq) (*CancelExecutionResp, error) {
	err := rc.api.CancelExecution(ctx, sqlexec.CancelExecutionRequest{})
	if err != nil {
		return nil, err
	}
	return &CancelExecutionResp{
		Status: &RequestStatus{
			StatusCode: "SUCCESS",
		},
	}, nil
}

var _ DatabricksClient = (*RestClient)(nil)

type RestHandle struct {
	StatementId string
}

func (h *RestHandle) Id() string {
	return h.StatementId
}

func toSchemaRest(s *sqlexec.ResultManifest) *ResultSchema {
	cols := []*ColumnInfo{}
	for _, c := range s.Schema.Columns {
		ti := columnTypeFromString(c.TypeText)
		if ti == UNKNOWN_TYPE {
			ti = columnTypeFromString(c.TypeName.String())
		}

		cols = append(cols, &ColumnInfo{
			Name:             c.Name,
			Position:         c.Position,
			Type:             ti,
			TypeName:         string(c.TypeName),
			TypeText:         c.TypeText,
			TypePrecision:    c.TypePrecision,
			TypeScale:        c.TypeScale,
			TypeIntervalType: c.TypeIntervalType,
		})
	}
	return &ResultSchema{
		Columns: cols,
	}
}
