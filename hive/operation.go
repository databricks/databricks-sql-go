package hive

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"time"

	"github.com/databricks/databricks-sql-go/cli_service"
)

// Operation represents hive operation
type Operation struct {
	hive            *Client
	h               *cli_service.TOperationHandle
	operationHelper *operationHelper
}

type operationHelper struct {
	state        *cli_service.TOperationState
	hasResultSet *bool
	// TODO: add status TGetOperationStatusResp
	schema      *TableSchema
	hasMoreRows *bool
	closed      *bool
	cancelled   *bool
}

// HasResultSet return if operation has result set
func (op *Operation) HasResultSet() bool {
	return op.h.GetHasResultSet()
}

// RowsAffected return number of rows affected by operation
func (op *Operation) RowsAffected() float64 {
	return op.h.GetModifiedRowCount()
}

// GetResultSetMetadata return schema
func (op *Operation) GetResultSetMetadata(ctx context.Context) (*TableSchema, error) {
	op.hive.log.Printf("fetch metadata for operation: %v", guid(op.h.OperationId.GUID))
	req := cli_service.TGetResultSetMetadataReq{
		OperationHandle: op.h,
	}

	resp, err := op.hive.client.GetResultSetMetadata(ctx, &req)
	if err != nil {
		return nil, err
	}
	if err := checkStatus(resp); err != nil {
		return nil, err
	}

	schema := new(TableSchema)

	if resp.IsSetSchema() {
		for _, desc := range resp.Schema.Columns {
			entry := desc.TypeDesc.Types[0].PrimitiveEntry

			dbtype := strings.TrimSuffix(entry.Type.String(), "_TYPE")
			schema.Columns = append(schema.Columns, &ColDesc{
				Name:             desc.ColumnName,
				DatabaseTypeName: dbtype,
				ScanType:         typeOf(entry),
			})
		}

		for _, col := range schema.Columns {
			op.hive.log.Printf("fetch schema: %v", col)
		}
	}

	return schema, nil
}

// FetchResults fetches query result from server
func (op *Operation) FetchResults(ctx context.Context, schema *TableSchema) (*ResultSet, error) {

	resp, err := fetch(ctx, op, schema)
	if err != nil {
		return nil, err
	}

	// check if operation has more rows
	hasMoreRows := op.checkIfOperationHasMoreRows(resp)
	op.operationHelper.hasMoreRows = &hasMoreRows

	rs := ResultSet{
		idx:     0,
		length:  length(resp.Results),
		result:  resp.Results,
		more:    resp.GetHasMoreRows(),
		schema:  schema,
		fetchfn: func() (*cli_service.TFetchResultsResp, error) { return fetch(ctx, op, schema) },
	}

	return &rs, nil
}

func fetch(ctx context.Context, op *Operation, schema *TableSchema) (*cli_service.TFetchResultsResp, error) {
	req := cli_service.TFetchResultsReq{
		OperationHandle: op.h,
		MaxRows:         op.hive.opts.MaxRows,
	}

	op.hive.log.Printf("fetch max rows: %v", op.hive.opts.MaxRows)

	op.hive.log.Printf("fetch results for operation: %v", guid(op.h.OperationId.GUID))

	resp, err := op.hive.client.FetchResults(ctx, &req)
	if err != nil {
		return nil, err
	}
	if err := checkStatus(resp); err != nil {
		return nil, err
	}

	op.hive.log.Printf("results: %v", resp.Results)
	return resp, nil
}

// Close closes operation
func (op *Operation) Close(ctx context.Context) error {
	req := cli_service.TCloseOperationReq{
		OperationHandle: op.h,
	}
	resp, err := op.hive.client.CloseOperation(ctx, &req)
	if err != nil {
		return err
	}
	if err := checkStatus(resp); err != nil {
		return err
	}

	op.hive.log.Printf("close operation: %v", guid(op.h.OperationId.GUID))
	return nil
}

// private method to cancel operation
func (op *Operation) cancel(ctx context.Context) error {
	req := cli_service.TCancelOperationReq{
		OperationHandle: op.h,
	}

	resp, err := op.hive.client.CancelOperation(ctx, &req)
	if err != nil {
		return err
	}
	if err := checkStatus(resp); err != nil {
		return err
	}

	op.hive.log.Printf("cancel operation: %v", guid(op.h.OperationId.GUID))
	return nil
}

func (op *Operation) FetchAll(ctx context.Context) ([]driver.Rows, error) {
	// TODO: pull this out to functional options
	operationStatus := &operationHelper{
		state:        new(cli_service.TOperationState),
		hasResultSet: new(bool),
	}
	op.operationHelper = operationStatus

	var data []driver.Rows
	for ok := true; ok; ok = op.hasMoreRows(ctx) {
		chunk, err := op.FetchChunk(ctx)
		if err != nil {
			return nil, err
		}
		data = append(data, chunk)
	}
	return data, nil
}

func (op *Operation) FetchChunk(ctx context.Context) (driver.Rows, error) {
	var data driver.Rows

	if !op.h.HasResultSet {
		return data, nil
	}

	err := op.waitUntilReady(ctx)
	if err != nil {
		return nil, err
	}

	schema, err := op.fetchMetadata(ctx)

	rs, err := op.FetchResults(ctx, schema)
	if err != nil {
		return nil, err
	}
	return &Rows{
		rs:      rs,
		schema:  schema,
		closefn: func() error { return op.Close(ctx) },
	}, nil
}

func (op *Operation) waitUntilReady(ctx context.Context) error {
	if *op.operationHelper.state == cli_service.TOperationState_FINISHED_STATE {
		return nil
	}

	newStatus, err := op.GetOperationStatus(ctx)
	if err != nil {
		return err
	}

	op.operationHelper.state = newStatus.OperationState
	op.operationHelper.hasResultSet = newStatus.HasResultSet

	// TODO: uncomment after understanding operationHelper member in Node
	//var inProgress bool
	//switch state := *newStatus.OperationState; state {
	//case cli_service.TOperationState_INITIALIZED_STATE,
	//cli_service.TOperationState_PENDING_STATE,
	//cli_service.TOperationState_RUNNING_STATE:
	//	inProgress = true
	//default:
	//	inProgress = false
	//}
	//if !inProgress {
	//	op.operationHelper.status = newStatus
	//}

	var isReady bool
	switch state := *newStatus.OperationState; state {
	case cli_service.TOperationState_INITIALIZED_STATE,
		cli_service.TOperationState_PENDING_STATE,
		cli_service.TOperationState_RUNNING_STATE:
		isReady = false
	case cli_service.TOperationState_FINISHED_STATE:
		isReady = true
	case cli_service.TOperationState_CANCELED_STATE:
		return errors.New("the operation was canceled by a client")
	case cli_service.TOperationState_CLOSED_STATE:
		return errors.New("the operation was closed by a client")
	case cli_service.TOperationState_ERROR_STATE:
		return errors.New("the operation failed due to an error")
	case cli_service.TOperationState_TIMEDOUT_STATE:
		return errors.New("the operation is in a timed out state")
	case cli_service.TOperationState_UKNOWN_STATE:
		return errors.New("the operation is in an unknown state")
	default:
		return errors.New("the operation is in an unrecognized state")
	}
	if !isReady {
		time.Sleep(100 * time.Millisecond)
		return op.waitUntilReady(ctx)
	}
	return nil
}

func (op *Operation) hasMoreRows(ctx context.Context) bool { // TODO: is this the right param and returns
	if *op.operationHelper.closed || *op.operationHelper.cancelled {
		return false
	}
	return *op.operationHelper.hasMoreRows
}

func (op *Operation) GetOperationStatus(ctx context.Context) (*cli_service.TGetOperationStatusResp, error) {
	req := cli_service.TGetOperationStatusReq{
		OperationHandle: op.h,
	}
	return op.hive.client.GetOperationStatus(ctx, &req)
}

func (op *Operation) fetchMetadata(ctx context.Context) (*TableSchema, error) {
	if op.operationHelper.schema != nil {
		return op.operationHelper.schema, nil
	}

	schema, err := op.GetResultSetMetadata(ctx)
	if err != nil {
		return nil, err
	}
	op.operationHelper.schema = schema
	return schema, nil
}

func (op *Operation) checkIfOperationHasMoreRows(resp *cli_service.TFetchResultsResp) bool {
	if *resp.HasMoreRows {
		return true
	}
	// TODO: not perfect match of implementation with Node
	if len(resp.Results.Columns) == 0 {
		return false
	}

	// TODO: implement columnValue check
	return false
}
