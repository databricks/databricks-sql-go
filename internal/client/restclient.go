package client

import (
	"context"
	"net/http"
	"strings"

	dbclient "github.com/databricks/databricks-sdk-go/client"
	sdkcfg "github.com/databricks/databricks-sdk-go/config"
	sqlexec "github.com/databricks/databricks-sdk-go/service/sql"
	"github.com/databricks/databricks-sql-go/internal/config"
)

type RestClient struct {
	cfg *config.Config
	api sqlexec.StatementExecutionService
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
	return nil, nil
}

func (rc *RestClient) FetchResults(ctx context.Context, req *FetchResultsReq) (*FetchResultsResp, error) {
	return nil, nil
}

func (rc *RestClient) GetResultsMetadata(ctx context.Context, req *GetResultsMetadataReq) (*GetResultsMetadataResp, error) {
	return nil, nil
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

	return &ExecuteStatementResp{
		ExecutionHandle: &RestHandle{
			StatementId: resp.StatementId,
		},
		ExecutionStatus: ExecutionStatus{
			ExecutionState: string(resp.Status.State),
			Error: &ExecutionError{
				Message:   resp.Status.Error.Message,
				ErrorCode: resp.Status.Error.ErrorCode.String(),
			},
		},
		// Schema: toSchemaFromManifest(),
		Result: nil,
	}, nil
}

func (rc *RestClient) GetExecutionStatus(ctx context.Context, req *GetExecutionStatusReq) (*GetExecutionStatusResp, error) {
	return nil, nil
}

func (rc *RestClient) CloseExecution(ctx context.Context, req *CloseExecutionReq) (*CloseExecutionResp, error) {
	return nil, nil
}

func (rc *RestClient) CancelExecution(ctx context.Context, req *CancelExecutionReq) (*CancelExecutionResp, error) {
	return nil, nil
}

var _ DatabricksClient = (*RestClient)(nil)

type RestHandle struct {
	StatementId string
}

func (h *RestHandle) Id() string {
	return h.StatementId
}

// func toSchemaFromManifest() *ResultSchema {

// }
