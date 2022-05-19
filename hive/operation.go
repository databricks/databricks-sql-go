package hive

import (
	"context"
	"strings"

	"github.com/databricks/databricks-sql-go/cli_service"
)

// Operation represents hive operation
type Operation struct {
	hive *Client
	h    *cli_service.TOperationHandle
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
