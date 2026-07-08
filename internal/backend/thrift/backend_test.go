package thrift

// White-box tests for the Thrift execute/poll state machine. They drive the
// unexported executeStatement/pollOperation/runQuery against a mock TCLIService.

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/databricks/databricks-sql-go/driverctx"
	"github.com/databricks/databricks-sql-go/internal/backend"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/databricks/databricks-sql-go/internal/thrift_protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// namedToParams converts driver.NamedValues to backend.Params with a minimal
// string-typed mapping — enough for these tests, which only exercise param
// passthrough, not the full type inference (that lives in the dbsql package).
func namedToParams(values []driver.NamedValue) []backend.Param {
	if len(values) == 0 {
		return nil
	}
	params := make([]backend.Param, 0, len(values))
	for _, v := range values {
		s := fmt.Sprintf("%v", v.Value)
		val := s
		params = append(params, backend.Param{Name: v.Name, Type: "STRING", Value: &val})
	}
	return params
}

func getTestSession() *cli_service.TOpenSessionResp {
	return &cli_service.TOpenSessionResp{SessionHandle: &cli_service.TSessionHandle{
		SessionId: &cli_service.THandleIdentifier{
			GUID: []byte{1, 2, 3, 4, 2, 23, 4, 2, 3, 2, 3, 4, 4, 223, 34, 54},
		},
	}}
}

func strPtr(s string) *string { return &s }

func TestBackend_executeStatement(t *testing.T) {
	t.Parallel()
	t.Run("executeStatement should err when client.ExecuteStatement fails", func(t *testing.T) {
		var executeStatementCount int
		executeStatement := func(ctx context.Context, req *cli_service.TExecuteStatementReq) (r *cli_service.TExecuteStatementResp, err error) {
			executeStatementCount++
			return nil, fmt.Errorf("error")
		}
		testClient := &client.TestClient{
			FnExecuteStatement: executeStatement,
		}
		be := NewForTest(testClient, getTestSession(), config.WithDefaults())
		_, err := be.executeStatement(context.Background(), backend.ExecRequest{Query: "select 1", Params: namedToParams([]driver.NamedValue{})})
		assert.Error(t, err)
		assert.Equal(t, 1, executeStatementCount)
	})

	t.Run("executeStatement should return TExecuteStatementResp on success", func(t *testing.T) {
		var executeStatementCount int
		executeStatement := func(ctx context.Context, req *cli_service.TExecuteStatementReq) (r *cli_service.TExecuteStatementResp, err error) {
			executeStatementCount++
			executeStatementResp := &cli_service.TExecuteStatementResp{
				Status: &cli_service.TStatus{
					StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
				},
				OperationHandle: &cli_service.TOperationHandle{
					OperationId: &cli_service.THandleIdentifier{
						GUID:   []byte{1, 2, 3, 4, 2, 23, 4, 2, 3, 1, 2, 3, 4, 4, 223, 34, 54},
						Secret: []byte("b"),
					},
				},
				DirectResults: &cli_service.TSparkDirectResults{
					OperationStatus: &cli_service.TGetOperationStatusResp{
						Status: &cli_service.TStatus{
							StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
						},
						OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_ERROR_STATE),
						ErrorMessage:   strPtr("error message"),
						DisplayMessage: strPtr("display message"),
					},
					ResultSetMetadata: &cli_service.TGetResultSetMetadataResp{
						Status: &cli_service.TStatus{
							StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
						},
					},
					ResultSet: &cli_service.TFetchResultsResp{
						Status: &cli_service.TStatus{
							StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
						},
					},
				},
			}
			return executeStatementResp, nil
		}
		testClient := &client.TestClient{
			FnExecuteStatement: executeStatement,
		}
		be := NewForTest(testClient, getTestSession(), config.WithDefaults())
		_, err := be.executeStatement(context.Background(), backend.ExecRequest{Query: "select 1", Params: namedToParams([]driver.NamedValue{})})

		assert.NoError(t, err)
		assert.Equal(t, 1, executeStatementCount)
	})

	t.Run("executeStatement should not call cancel if not needed", func(t *testing.T) {
		var executeStatementCount int
		var cancelOperationCount int
		var cancel context.CancelFunc
		executeStatement := func(ctx context.Context, req *cli_service.TExecuteStatementReq) (r *cli_service.TExecuteStatementResp, err error) {
			executeStatementCount++
			cancel()
			executeStatementResp := &cli_service.TExecuteStatementResp{
				Status: &cli_service.TStatus{
					StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
				},
				OperationHandle: &cli_service.TOperationHandle{
					OperationId: &cli_service.THandleIdentifier{
						GUID:   []byte{1, 2, 3, 4, 2, 23, 4, 2, 3, 1, 2, 3, 4, 4, 223, 34, 54},
						Secret: []byte("b"),
					},
				},
				DirectResults: &cli_service.TSparkDirectResults{
					OperationStatus: &cli_service.TGetOperationStatusResp{
						Status: &cli_service.TStatus{
							StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
						},
						OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_FINISHED_STATE),
						ErrorMessage:   strPtr("error message"),
						DisplayMessage: strPtr("display message"),
					},
					ResultSetMetadata: &cli_service.TGetResultSetMetadataResp{
						Status: &cli_service.TStatus{
							StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
						},
					},
					ResultSet: &cli_service.TFetchResultsResp{
						Status: &cli_service.TStatus{
							StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
						},
					},
					CloseOperation: &cli_service.TCloseOperationResp{
						Status: &cli_service.TStatus{
							StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
						},
					},
				},
			}
			return executeStatementResp, nil
		}
		cancelOperation := func(ctx context.Context, req *cli_service.TCancelOperationReq) (r *cli_service.TCancelOperationResp, err error) {
			cancelOperationCount++
			cancelOperationResp := &cli_service.TCancelOperationResp{
				Status: &cli_service.TStatus{
					StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
				},
			}
			return cancelOperationResp, nil
		}
		testClient := &client.TestClient{
			FnExecuteStatement: executeStatement,
			FnCancelOperation:  cancelOperation,
		}
		be := NewForTest(testClient, getTestSession(), config.WithDefaults())

		ctx := context.Background()
		ctx, cancel = context.WithCancel(ctx)
		defer cancel()
		_, err := be.executeStatement(ctx, backend.ExecRequest{Query: "select 1", Params: namedToParams([]driver.NamedValue{})})

		assert.Error(t, err)
		assert.Equal(t, 1, executeStatementCount)
		assert.Equal(t, 0, cancelOperationCount)
	})
	t.Run("executeStatement should call cancel if needed", func(t *testing.T) {
		var executeStatementCount int
		var cancelOperationCount int
		var cancel context.CancelFunc
		executeStatement := func(ctx context.Context, req *cli_service.TExecuteStatementReq) (r *cli_service.TExecuteStatementResp, err error) {
			executeStatementCount++
			cancel()
			executeStatementResp := &cli_service.TExecuteStatementResp{
				Status: &cli_service.TStatus{
					StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
				},
				OperationHandle: &cli_service.TOperationHandle{
					OperationId: &cli_service.THandleIdentifier{
						GUID:   []byte{1, 2, 3, 4, 2, 23, 4, 2, 3, 1, 2, 3, 4, 4, 223, 34, 54},
						Secret: []byte("b"),
					},
				},
				DirectResults: &cli_service.TSparkDirectResults{
					OperationStatus: &cli_service.TGetOperationStatusResp{
						Status: &cli_service.TStatus{
							StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
						},
						OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_FINISHED_STATE),
						ErrorMessage:   strPtr("error message"),
						DisplayMessage: strPtr("display message"),
					},
					ResultSetMetadata: &cli_service.TGetResultSetMetadataResp{
						Status: &cli_service.TStatus{
							StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
						},
					},
					ResultSet: &cli_service.TFetchResultsResp{
						Status: &cli_service.TStatus{
							StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
						},
					},
				},
			}
			return executeStatementResp, nil
		}
		cancelOperation := func(ctx context.Context, req *cli_service.TCancelOperationReq) (r *cli_service.TCancelOperationResp, err error) {
			cancelOperationCount++
			cancelOperationResp := &cli_service.TCancelOperationResp{
				Status: &cli_service.TStatus{
					StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
				},
			}
			return cancelOperationResp, nil
		}
		testClient := &client.TestClient{
			FnExecuteStatement: executeStatement,
			FnCancelOperation:  cancelOperation,
		}
		be := NewForTest(testClient, getTestSession(), config.WithDefaults())
		ctx := context.Background()
		ctx, cancel = context.WithCancel(ctx)
		defer cancel()
		_, err := be.executeStatement(ctx, backend.ExecRequest{Query: "select 1", Params: namedToParams([]driver.NamedValue{})})

		assert.Error(t, err)
		assert.Equal(t, 1, executeStatementCount)
		assert.Equal(t, 1, cancelOperationCount)
	})

}

func TestBackend_executeStatement_ProtocolFeatures(t *testing.T) {
	t.Parallel()

	protocols := []cli_service.TProtocolVersion{
		cli_service.TProtocolVersion_SPARK_CLI_SERVICE_PROTOCOL_V1,
		cli_service.TProtocolVersion_SPARK_CLI_SERVICE_PROTOCOL_V2,
		cli_service.TProtocolVersion_SPARK_CLI_SERVICE_PROTOCOL_V3,
		cli_service.TProtocolVersion_SPARK_CLI_SERVICE_PROTOCOL_V4,
		cli_service.TProtocolVersion_SPARK_CLI_SERVICE_PROTOCOL_V5,
		cli_service.TProtocolVersion_SPARK_CLI_SERVICE_PROTOCOL_V6,
		cli_service.TProtocolVersion_SPARK_CLI_SERVICE_PROTOCOL_V7,
		cli_service.TProtocolVersion_SPARK_CLI_SERVICE_PROTOCOL_V8,
	}

	testCases := []struct {
		cfg                          *config.Config
		supportsDirectResults        func(version cli_service.TProtocolVersion) bool
		supportsLz4Compression       func(version cli_service.TProtocolVersion) bool
		supportsCloudFetch           func(version cli_service.TProtocolVersion) bool
		supportsArrow                func(version cli_service.TProtocolVersion) bool
		supportsParameterizedQueries func(version cli_service.TProtocolVersion) bool
		hasParameters                bool
	}{
		{
			cfg: func() *config.Config {
				cfg := config.WithDefaults()
				cfg.UseLz4Compression = true
				cfg.UseCloudFetch = true
				cfg.UseArrowBatches = true
				cfg.UseArrowNativeDecimal = true
				cfg.UseArrowNativeTimestamp = true
				cfg.UseArrowNativeComplexTypes = true
				cfg.UseArrowNativeIntervalTypes = true
				return cfg
			}(),
			supportsDirectResults:        thrift_protocol.SupportsDirectResults,
			supportsLz4Compression:       thrift_protocol.SupportsLz4Compression,
			supportsCloudFetch:           thrift_protocol.SupportsCloudFetch,
			supportsArrow:                thrift_protocol.SupportsArrow,
			supportsParameterizedQueries: thrift_protocol.SupportsParameterizedQueries,
			hasParameters:                true,
		},
		{
			cfg: func() *config.Config {
				cfg := config.WithDefaults()
				cfg.UseLz4Compression = false
				cfg.UseCloudFetch = false
				cfg.UseArrowBatches = false
				return cfg
			}(),
			supportsDirectResults:        thrift_protocol.SupportsDirectResults,
			supportsLz4Compression:       thrift_protocol.SupportsLz4Compression,
			supportsCloudFetch:           thrift_protocol.SupportsCloudFetch,
			supportsArrow:                thrift_protocol.SupportsArrow,
			supportsParameterizedQueries: thrift_protocol.SupportsParameterizedQueries,
			hasParameters:                false,
		},
	}

	for _, tc := range testCases {
		for _, version := range protocols {
			t.Run(fmt.Sprintf("protocol_v%d_withParams_%v", version, tc.hasParameters), func(t *testing.T) {
				var capturedReq *cli_service.TExecuteStatementReq
				executeStatement := func(ctx context.Context, req *cli_service.TExecuteStatementReq) (r *cli_service.TExecuteStatementResp, err error) {
					capturedReq = req
					executeStatementResp := &cli_service.TExecuteStatementResp{
						Status: &cli_service.TStatus{
							StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
						},
						OperationHandle: &cli_service.TOperationHandle{
							OperationId: &cli_service.THandleIdentifier{
								GUID:   []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
								Secret: []byte("secret"),
							},
						},
						DirectResults: &cli_service.TSparkDirectResults{
							OperationStatus: &cli_service.TGetOperationStatusResp{
								Status: &cli_service.TStatus{
									StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
								},
								OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_FINISHED_STATE),
							},
						},
					}
					return executeStatementResp, nil
				}

				session := getTestSession()
				session.ServerProtocolVersion = version

				testClient := &client.TestClient{
					FnExecuteStatement: executeStatement,
				}

				be := NewForTest(testClient, session, tc.cfg)

				var args []driver.NamedValue
				if tc.hasParameters {
					args = []driver.NamedValue{
						{Name: "param1", Value: "value1"},
					}
				}

				_, err := be.executeStatement(context.Background(), backend.ExecRequest{Query: "SELECT 1", Params: namedToParams(args)})
				assert.NoError(t, err)

				// Verify direct results
				hasDirectResults := tc.supportsDirectResults(version)
				assert.Equal(t, hasDirectResults, capturedReq.GetDirectResults != nil, "Direct results should be enabled if protocol supports it")

				// Verify LZ4 compression
				shouldHaveLz4 := tc.supportsLz4Compression(version) && tc.cfg.UseLz4Compression
				if shouldHaveLz4 {
					assert.NotNil(t, capturedReq.CanDecompressLZ4Result_)
					assert.True(t, *capturedReq.CanDecompressLZ4Result_)
				} else {
					assert.Nil(t, capturedReq.CanDecompressLZ4Result_)
				}

				// Verify cloud fetch
				shouldHaveCloudFetch := tc.supportsCloudFetch(version) && tc.cfg.UseCloudFetch
				if shouldHaveCloudFetch {
					assert.NotNil(t, capturedReq.CanDownloadResult_)
					assert.True(t, *capturedReq.CanDownloadResult_)
				} else {
					assert.Nil(t, capturedReq.CanDownloadResult_)
				}

				// Verify Arrow support
				shouldHaveArrow := tc.supportsArrow(version) && tc.cfg.UseArrowBatches
				if shouldHaveArrow {
					assert.NotNil(t, capturedReq.CanReadArrowResult_)
					assert.True(t, *capturedReq.CanReadArrowResult_)
					assert.NotNil(t, capturedReq.UseArrowNativeTypes)
					assert.Equal(t, tc.cfg.UseArrowNativeDecimal, *capturedReq.UseArrowNativeTypes.DecimalAsArrow)
					assert.Equal(t, tc.cfg.UseArrowNativeTimestamp, *capturedReq.UseArrowNativeTypes.TimestampAsArrow)
					assert.Equal(t, tc.cfg.UseArrowNativeComplexTypes, *capturedReq.UseArrowNativeTypes.ComplexTypesAsArrow)
					assert.Equal(t, tc.cfg.UseArrowNativeIntervalTypes, *capturedReq.UseArrowNativeTypes.IntervalTypesAsArrow)
				} else {
					assert.Nil(t, capturedReq.CanReadArrowResult_)
					assert.Nil(t, capturedReq.UseArrowNativeTypes)
				}

				// Verify parameters
				shouldHaveParams := tc.supportsParameterizedQueries(version) && tc.hasParameters
				if shouldHaveParams {
					assert.NotNil(t, capturedReq.Parameters)
					assert.Len(t, capturedReq.Parameters, 1)
				} else if tc.hasParameters {
					// Even if we have parameters but protocol doesn't support it, we shouldn't set them
					assert.Nil(t, capturedReq.Parameters)
				}
			})
		}
	}
}

func TestBackend_executeStatement_QueryTags(t *testing.T) {
	t.Parallel()

	makeTestBackend := func(captureReq *(*cli_service.TExecuteStatementReq)) *Backend {
		executeStatement := func(ctx context.Context, req *cli_service.TExecuteStatementReq) (r *cli_service.TExecuteStatementResp, err error) {
			*captureReq = req
			return &cli_service.TExecuteStatementResp{
				Status: &cli_service.TStatus{
					StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
				},
				OperationHandle: &cli_service.TOperationHandle{
					OperationId: &cli_service.THandleIdentifier{
						GUID:   []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
						Secret: []byte("secret"),
					},
				},
				DirectResults: &cli_service.TSparkDirectResults{
					OperationStatus: &cli_service.TGetOperationStatusResp{
						Status: &cli_service.TStatus{
							StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
						},
						OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_FINISHED_STATE),
					},
				},
			}, nil
		}

		return NewForTest(
			&client.TestClient{FnExecuteStatement: executeStatement},
			getTestSession(),
			config.WithDefaults(),
		)
	}

	t.Run("query tags from context are set in ConfOverlay", func(t *testing.T) {
		var capturedReq *cli_service.TExecuteStatementReq
		be := makeTestBackend(&capturedReq)

		ctx := driverctx.NewContextWithQueryTags(context.Background(), map[string]string{
			"team": "engineering",
			"app":  "etl",
		})

		_, err := be.executeStatement(ctx, backend.ExecRequest{Query: "SELECT 1", Params: namedToParams(nil)})
		assert.NoError(t, err)
		assert.NotNil(t, capturedReq.ConfOverlay)
		// Map iteration is non-deterministic, so check both possible orderings
		queryTags := capturedReq.ConfOverlay["query_tags"]
		assert.True(t,
			queryTags == "team:engineering,app:etl" || queryTags == "app:etl,team:engineering",
			"unexpected query_tags value: %s", queryTags)
	})

	t.Run("no query tags in context means no ConfOverlay", func(t *testing.T) {
		var capturedReq *cli_service.TExecuteStatementReq
		be := makeTestBackend(&capturedReq)

		_, err := be.executeStatement(context.Background(), backend.ExecRequest{Query: "SELECT 1", Params: namedToParams(nil)})
		assert.NoError(t, err)
		assert.Nil(t, capturedReq.ConfOverlay)
	})

	t.Run("empty query tags map means no ConfOverlay", func(t *testing.T) {
		var capturedReq *cli_service.TExecuteStatementReq
		be := makeTestBackend(&capturedReq)

		ctx := driverctx.NewContextWithQueryTags(context.Background(), map[string]string{})

		_, err := be.executeStatement(ctx, backend.ExecRequest{Query: "SELECT 1", Params: namedToParams(nil)})
		assert.NoError(t, err)
		assert.Nil(t, capturedReq.ConfOverlay)
	})

	t.Run("single query tag", func(t *testing.T) {
		var capturedReq *cli_service.TExecuteStatementReq
		be := makeTestBackend(&capturedReq)

		ctx := driverctx.NewContextWithQueryTags(context.Background(), map[string]string{
			"team": "data-eng",
		})

		_, err := be.executeStatement(ctx, backend.ExecRequest{Query: "SELECT 1", Params: namedToParams(nil)})
		assert.NoError(t, err)
		assert.Equal(t, "team:data-eng", capturedReq.ConfOverlay["query_tags"])
	})

	t.Run("query tags with special characters in values", func(t *testing.T) {
		var capturedReq *cli_service.TExecuteStatementReq
		be := makeTestBackend(&capturedReq)

		ctx := driverctx.NewContextWithQueryTags(context.Background(), map[string]string{
			"url": "http://host:8080",
		})

		_, err := be.executeStatement(ctx, backend.ExecRequest{Query: "SELECT 1", Params: namedToParams(nil)})
		assert.NoError(t, err)
		assert.Equal(t, `url:http\://host\:8080`, capturedReq.ConfOverlay["query_tags"])
	})

	t.Run("query tags with empty value", func(t *testing.T) {
		var capturedReq *cli_service.TExecuteStatementReq
		be := makeTestBackend(&capturedReq)

		ctx := driverctx.NewContextWithQueryTags(context.Background(), map[string]string{
			"flag": "",
		})

		_, err := be.executeStatement(ctx, backend.ExecRequest{Query: "SELECT 1", Params: namedToParams(nil)})
		assert.NoError(t, err)
		assert.Equal(t, "flag", capturedReq.ConfOverlay["query_tags"])
	})

	t.Run("session-level and statement-level query tags coexist", func(t *testing.T) {
		// Session-level tags are sent via TOpenSessionReq.Configuration at connect time.
		// Statement-level tags are sent via TExecuteStatementReq.ConfOverlay at query time.
		// They are independent fields on different requests, so both should work together.

		var capturedOpenReq *cli_service.TOpenSessionReq
		var capturedExecReq *cli_service.TExecuteStatementReq

		testClient := &client.TestClient{
			FnOpenSession: func(ctx context.Context, req *cli_service.TOpenSessionReq) (*cli_service.TOpenSessionResp, error) {
				capturedOpenReq = req
				return &cli_service.TOpenSessionResp{
					Status: &cli_service.TStatus{
						StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
					},
					SessionHandle: &cli_service.TSessionHandle{
						SessionId: &cli_service.THandleIdentifier{
							GUID: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
						},
					},
				}, nil
			},
			FnExecuteStatement: func(ctx context.Context, req *cli_service.TExecuteStatementReq) (*cli_service.TExecuteStatementResp, error) {
				capturedExecReq = req
				return &cli_service.TExecuteStatementResp{
					Status: &cli_service.TStatus{
						StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
					},
					OperationHandle: &cli_service.TOperationHandle{
						OperationId: &cli_service.THandleIdentifier{
							GUID:   []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
							Secret: []byte("secret"),
						},
					},
					DirectResults: &cli_service.TSparkDirectResults{
						OperationStatus: &cli_service.TGetOperationStatusResp{
							Status: &cli_service.TStatus{
								StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
							},
							OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_FINISHED_STATE),
						},
					},
				}, nil
			},
		}

		// Simulate what connector.Connect() does: pass session params to OpenSession
		sessionParams := map[string]string{
			"QUERY_TAGS": "team:platform,env:prod",
			"ansi_mode":  "false",
		}
		protocolVersion := int64(cli_service.TProtocolVersion_SPARK_CLI_SERVICE_PROTOCOL_V8)
		session, err := testClient.OpenSession(context.Background(), &cli_service.TOpenSessionReq{
			ClientProtocolI64: &protocolVersion,
			Configuration:     sessionParams,
		})
		assert.NoError(t, err)

		// Verify session-level tags were sent in OpenSession
		assert.Equal(t, "team:platform,env:prod", capturedOpenReq.Configuration["QUERY_TAGS"])
		assert.Equal(t, "false", capturedOpenReq.Configuration["ansi_mode"])

		// Create conn with session that has session-level tags
		cfg := config.WithDefaults()
		cfg.SessionParams = sessionParams
		be := NewForTest(testClient, session, cfg)

		// Execute with statement-level tags
		ctx := driverctx.NewContextWithQueryTags(context.Background(), map[string]string{
			"job": "nightly-etl",
		})
		_, err = be.executeStatement(ctx, backend.ExecRequest{Query: "SELECT 1", Params: namedToParams(nil)})
		assert.NoError(t, err)

		// Statement-level tags should be in ConfOverlay
		assert.Equal(t, "job:nightly-etl", capturedExecReq.ConfOverlay["query_tags"])

		// ConfOverlay should ONLY have query_tags, not session params
		_, hasAnsiMode := capturedExecReq.ConfOverlay["ansi_mode"]
		assert.False(t, hasAnsiMode, "session params should not leak into ConfOverlay")
		_, hasSessionQueryTags := capturedExecReq.ConfOverlay["QUERY_TAGS"]
		assert.False(t, hasSessionQueryTags, "session-level QUERY_TAGS should not be in ConfOverlay")
	})
}

func TestBackend_pollOperation(t *testing.T) {
	t.Parallel()
	t.Run("pollOperation returns finished state response when query finishes", func(t *testing.T) {
		var getOperationStatusCount int
		getOperationStatus := func(ctx context.Context, req *cli_service.TGetOperationStatusReq) (r *cli_service.TGetOperationStatusResp, err error) {
			getOperationStatusCount++
			getOperationStatusResp := &cli_service.TGetOperationStatusResp{
				OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_FINISHED_STATE),
			}
			return getOperationStatusResp, nil
		}
		testClient := &client.TestClient{
			FnGetOperationStatus: getOperationStatus,
		}
		be := NewForTest(testClient, getTestSession(), config.WithDefaults())
		res, err := be.pollOperation(context.Background(), &cli_service.TOperationHandle{
			OperationId: &cli_service.THandleIdentifier{
				GUID:   []byte{1, 2, 3, 4, 2, 23, 4, 2, 3, 2, 4, 7, 8, 223, 34, 54},
				Secret: []byte("b"),
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, getOperationStatusCount)
		assert.Equal(t, cli_service.TGetOperationStatusResp{
			OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_FINISHED_STATE),
		}, *res)
	})

	t.Run("pollOperation returns closed state response when query has been closed", func(t *testing.T) {
		var getOperationStatusCount int
		getOperationStatus := func(ctx context.Context, req *cli_service.TGetOperationStatusReq) (r *cli_service.TGetOperationStatusResp, err error) {
			getOperationStatusCount++
			getOperationStatusResp := &cli_service.TGetOperationStatusResp{
				OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_CLOSED_STATE),
			}
			return getOperationStatusResp, nil
		}
		testClient := &client.TestClient{
			FnGetOperationStatus: getOperationStatus,
		}
		be := NewForTest(testClient, getTestSession(), config.WithDefaults())
		res, err := be.pollOperation(context.Background(), &cli_service.TOperationHandle{
			OperationId: &cli_service.THandleIdentifier{
				GUID:   []byte{1, 2, 3, 4, 2, 23, 4, 2, 3, 2, 3, 4, 4, 223, 34, 54},
				Secret: []byte("b"),
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, getOperationStatusCount)
		assert.Equal(t, cli_service.TGetOperationStatusResp{
			OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_CLOSED_STATE),
		}, *res)
	})

	t.Run("pollOperation returns closed state response when query has been closed", func(t *testing.T) {
		var getOperationStatusCount int
		getOperationStatus := func(ctx context.Context, req *cli_service.TGetOperationStatusReq) (r *cli_service.TGetOperationStatusResp, err error) {
			getOperationStatusCount++
			getOperationStatusResp := &cli_service.TGetOperationStatusResp{
				OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_CLOSED_STATE),
			}
			return getOperationStatusResp, nil
		}
		testClient := &client.TestClient{
			FnGetOperationStatus: getOperationStatus,
		}
		be := NewForTest(testClient, getTestSession(), config.WithDefaults())
		res, err := be.pollOperation(context.Background(), &cli_service.TOperationHandle{
			OperationId: &cli_service.THandleIdentifier{
				GUID:   []byte{1, 2, 3, 4, 2, 23, 4, 2, 3, 2, 3, 4, 4, 223, 34, 54},
				Secret: []byte("b"),
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, getOperationStatusCount)
		assert.Equal(t, cli_service.TGetOperationStatusResp{
			OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_CLOSED_STATE),
		}, *res)
	})

	t.Run("pollOperation returns unknown state response when query state is unknown", func(t *testing.T) {
		var getOperationStatusCount int
		getOperationStatus := func(ctx context.Context, req *cli_service.TGetOperationStatusReq) (r *cli_service.TGetOperationStatusResp, err error) {
			getOperationStatusCount++
			getOperationStatusResp := &cli_service.TGetOperationStatusResp{
				OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_UKNOWN_STATE),
			}
			return getOperationStatusResp, nil
		}
		testClient := &client.TestClient{
			FnGetOperationStatus: getOperationStatus,
		}
		be := NewForTest(testClient, getTestSession(), config.WithDefaults())
		res, err := be.pollOperation(context.Background(), &cli_service.TOperationHandle{
			OperationId: &cli_service.THandleIdentifier{
				GUID:   []byte{1, 2, 3, 4, 2, 23, 4, 2, 3, 1, 2, 3, 4, 223, 34, 54},
				Secret: []byte("b"),
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, getOperationStatusCount)
		assert.Equal(t, cli_service.TGetOperationStatusResp{
			OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_UKNOWN_STATE),
		}, *res)
	})

	t.Run("pollOperation returns error state response when query errors", func(t *testing.T) {
		var getOperationStatusCount int
		getOperationStatus := func(ctx context.Context, req *cli_service.TGetOperationStatusReq) (r *cli_service.TGetOperationStatusResp, err error) {
			getOperationStatusCount++
			getOperationStatusResp := &cli_service.TGetOperationStatusResp{
				OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_ERROR_STATE),
			}
			return getOperationStatusResp, nil
		}
		testClient := &client.TestClient{
			FnGetOperationStatus: getOperationStatus,
		}
		be := NewForTest(testClient, getTestSession(), config.WithDefaults())
		res, err := be.pollOperation(context.Background(), &cli_service.TOperationHandle{
			OperationId: &cli_service.THandleIdentifier{
				GUID:   []byte{1, 2, 3, 4, 2, 23, 4, 2, 3, 1, 2, 4, 4, 223, 34, 54},
				Secret: []byte("b"),
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, getOperationStatusCount)
		assert.Equal(t, cli_service.TGetOperationStatusResp{
			OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_ERROR_STATE),
		}, *res)
	})

	t.Run("pollOperation returns finished state response after query cycles through various states", func(t *testing.T) {
		var getOperationStatusCount int
		getOperationStatus := func(ctx context.Context, req *cli_service.TGetOperationStatusReq) (r *cli_service.TGetOperationStatusResp, err error) {
			operationStates := [4]cli_service.TOperationState{cli_service.TOperationState_INITIALIZED_STATE, cli_service.TOperationState_PENDING_STATE,
				cli_service.TOperationState_RUNNING_STATE, cli_service.TOperationState_FINISHED_STATE}
			getOperationStatusCount++
			getOperationStatusResp := &cli_service.TGetOperationStatusResp{
				OperationState: cli_service.TOperationStatePtr(operationStates[getOperationStatusCount-1]),
			}
			return getOperationStatusResp, nil
		}
		testClient := &client.TestClient{
			FnGetOperationStatus: getOperationStatus,
		}
		be := NewForTest(testClient, getTestSession(), config.WithDefaults())
		res, err := be.pollOperation(context.Background(), &cli_service.TOperationHandle{
			OperationId: &cli_service.THandleIdentifier{
				GUID:   []byte{1, 2, 3, 4, 2, 23, 4, 2, 3, 1, 3, 4, 4, 223, 34, 54},
				Secret: []byte("b"),
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, 4, getOperationStatusCount)
		assert.Equal(t, cli_service.TGetOperationStatusResp{
			OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_FINISHED_STATE),
		}, *res)
	})

	t.Run("pollOperation returns cancel err when context times out before get operation", func(t *testing.T) {
		var getOperationStatusCount, cancelOperationCount int
		getOperationStatus := func(ctx context.Context, req *cli_service.TGetOperationStatusReq) (r *cli_service.TGetOperationStatusResp, err error) {
			operationStates := [4]cli_service.TOperationState{cli_service.TOperationState_INITIALIZED_STATE, cli_service.TOperationState_PENDING_STATE,
				cli_service.TOperationState_RUNNING_STATE, cli_service.TOperationState_FINISHED_STATE}
			getOperationStatusCount++
			getOperationStatusResp := &cli_service.TGetOperationStatusResp{
				OperationState: cli_service.TOperationStatePtr(operationStates[getOperationStatusCount-1]),
			}
			return getOperationStatusResp, nil
		}
		cancelOperation := func(ctx context.Context, req *cli_service.TCancelOperationReq) (r *cli_service.TCancelOperationResp, err error) {
			cancelOperationCount++
			cancelOperationResp := &cli_service.TCancelOperationResp{
				Status: &cli_service.TStatus{
					StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
				},
			}
			return cancelOperationResp, nil
		}
		testClient := &client.TestClient{
			FnGetOperationStatus: getOperationStatus,
			FnCancelOperation:    cancelOperation,
		}
		be := NewForTest(testClient, getTestSession(), config.WithDefaults())
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		res, err := be.pollOperation(ctx, &cli_service.TOperationHandle{

			OperationId: &cli_service.THandleIdentifier{
				GUID:   []byte{1, 2, 3, 4, 2, 23, 4, 2, 3, 1, 2, 4, 4, 223, 34, 54},
				Secret: []byte("b"),
			},
		})
		assert.Error(t, err)
		assert.Equal(t, 0, getOperationStatusCount)
		assert.Equal(t, 1, cancelOperationCount)
		assert.Nil(t, res)
	})

	t.Run("pollOperation returns cancel err when context times out before get operation", func(t *testing.T) {
		var getOperationStatusCount, cancelOperationCount int
		getOperationStatus := func(ctx context.Context, req *cli_service.TGetOperationStatusReq) (r *cli_service.TGetOperationStatusResp, err error) {
			operationStates := [4]cli_service.TOperationState{cli_service.TOperationState_INITIALIZED_STATE, cli_service.TOperationState_PENDING_STATE,
				cli_service.TOperationState_RUNNING_STATE, cli_service.TOperationState_FINISHED_STATE}
			getOperationStatusCount++
			getOperationStatusResp := &cli_service.TGetOperationStatusResp{
				OperationState: cli_service.TOperationStatePtr(operationStates[getOperationStatusCount-1]),
			}
			return getOperationStatusResp, nil
		}
		cancelOperation := func(ctx context.Context, req *cli_service.TCancelOperationReq) (r *cli_service.TCancelOperationResp, err error) {
			cancelOperationCount++
			cancelOperationResp := &cli_service.TCancelOperationResp{
				Status: &cli_service.TStatus{
					StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
				},
			}
			return cancelOperationResp, nil
		}
		testClient := &client.TestClient{
			FnGetOperationStatus: getOperationStatus,
			FnCancelOperation:    cancelOperation,
		}
		cfg := config.WithDefaults()
		cfg.PollInterval = 100 * time.Millisecond
		be := NewForTest(testClient, getTestSession(), cfg)
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		defer cancel()
		res, err := be.pollOperation(ctx, &cli_service.TOperationHandle{
			OperationId: &cli_service.THandleIdentifier{
				GUID:   []byte{1, 2, 3, 4, 2, 23, 4, 2, 3, 1, 2, 3, 4, 223, 34, 54},
				Secret: []byte("b"),
			},
		})

		assert.Error(t, err)
		assert.GreaterOrEqual(t, getOperationStatusCount, 1)
		assert.Equal(t, 1, cancelOperationCount)
		assert.Nil(t, res)
	})

	t.Run("pollOperation returns cancel err when context is cancelled", func(t *testing.T) {
		var getOperationStatusCount, cancelOperationCount int
		getOperationStatus := func(ctx context.Context, req *cli_service.TGetOperationStatusReq) (r *cli_service.TGetOperationStatusResp, err error) {
			operationStates := [4]cli_service.TOperationState{cli_service.TOperationState_INITIALIZED_STATE, cli_service.TOperationState_PENDING_STATE,
				cli_service.TOperationState_RUNNING_STATE, cli_service.TOperationState_FINISHED_STATE}
			getOperationStatusCount++
			getOperationStatusResp := &cli_service.TGetOperationStatusResp{
				OperationState: cli_service.TOperationStatePtr(operationStates[getOperationStatusCount-1]),
			}
			return getOperationStatusResp, nil
		}
		cancelOperation := func(ctx context.Context, req *cli_service.TCancelOperationReq) (r *cli_service.TCancelOperationResp, err error) {
			cancelOperationCount++
			cancelOperationResp := &cli_service.TCancelOperationResp{
				Status: &cli_service.TStatus{
					StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
				},
			}
			return cancelOperationResp, nil
		}
		testClient := &client.TestClient{
			FnGetOperationStatus: getOperationStatus,
			FnCancelOperation:    cancelOperation,
		}
		cfg := config.WithDefaults()
		cfg.PollInterval = 100 * time.Millisecond
		be := NewForTest(testClient, getTestSession(), cfg)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() {
			time.Sleep(150 * time.Millisecond)
			cancel()
		}()
		res, err := be.pollOperation(ctx, &cli_service.TOperationHandle{
			OperationId: &cli_service.THandleIdentifier{
				GUID:   []byte{1, 2, 3, 4, 2, 23, 4, 2, 3, 1, 2, 4, 4, 223, 34, 54},
				Secret: []byte("b"),
			},
		})
		assert.Error(t, err)
		assert.GreaterOrEqual(t, getOperationStatusCount, 1)
		assert.GreaterOrEqual(t, 1, cancelOperationCount)
		assert.Nil(t, res)
	})
}

func TestBackend_runQuery(t *testing.T) {
	t.Parallel()
	t.Run("runQuery should err when client.ExecuteStatement fails", func(t *testing.T) {
		var executeStatementCount int
		executeStatement := func(ctx context.Context, req *cli_service.TExecuteStatementReq) (r *cli_service.TExecuteStatementResp, err error) {
			executeStatementCount++
			return nil, fmt.Errorf("error")
		}
		testClient := &client.TestClient{
			FnExecuteStatement: executeStatement,
		}
		be := NewForTest(testClient, getTestSession(), config.WithDefaults())
		exStmtResp, opStatusResp, err := be.runQuery(context.Background(), backend.ExecRequest{Query: "select 1", Params: namedToParams([]driver.NamedValue{})})
		assert.Error(t, err)
		assert.Nil(t, exStmtResp)
		assert.Nil(t, opStatusResp)
		assert.Equal(t, 1, executeStatementCount)
	})

	t.Run("runQuery should err when pollOperation fails", func(t *testing.T) {
		var executeStatementCount, getOperationStatusCount int
		executeStatement := func(ctx context.Context, req *cli_service.TExecuteStatementReq) (r *cli_service.TExecuteStatementResp, err error) {
			executeStatementCount++
			executeStatementResp := &cli_service.TExecuteStatementResp{
				Status: &cli_service.TStatus{
					StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
				},
				OperationHandle: &cli_service.TOperationHandle{
					OperationId: &cli_service.THandleIdentifier{
						GUID:   []byte{1, 2, 3, 4, 2, 23, 4, 2, 3, 1, 3, 4, 4, 223, 34, 54},
						Secret: []byte("b"),
					},
				},
			}
			return executeStatementResp, nil
		}

		getOperationStatus := func(ctx context.Context, req *cli_service.TGetOperationStatusReq) (r *cli_service.TGetOperationStatusResp, err error) {
			getOperationStatusCount++
			getOperationStatusResp := &cli_service.TGetOperationStatusResp{
				OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_ERROR_STATE),
			}
			return getOperationStatusResp, fmt.Errorf("error on get operation status")
		}

		testClient := &client.TestClient{
			FnExecuteStatement:   executeStatement,
			FnGetOperationStatus: getOperationStatus,
		}
		be := NewForTest(testClient, getTestSession(), config.WithDefaults())
		exStmtResp, opStatusResp, err := be.runQuery(context.Background(), backend.ExecRequest{Query: "select 1", Params: namedToParams([]driver.NamedValue{})})

		assert.Error(t, err)
		assert.Equal(t, 1, executeStatementCount)
		assert.Equal(t, 1, getOperationStatusCount)
		assert.NotNil(t, exStmtResp)
		assert.Nil(t, opStatusResp)
	})

	t.Run("runQuery should return resp when query is finished", func(t *testing.T) {
		var executeStatementCount, getOperationStatusCount int
		executeStatement := func(ctx context.Context, req *cli_service.TExecuteStatementReq) (r *cli_service.TExecuteStatementResp, err error) {
			executeStatementCount++
			executeStatementResp := &cli_service.TExecuteStatementResp{
				Status: &cli_service.TStatus{
					StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
				},
				OperationHandle: &cli_service.TOperationHandle{
					OperationId: &cli_service.THandleIdentifier{
						GUID:   []byte{1, 2, 3, 4, 2, 23, 4, 2, 3, 1, 2, 4, 4, 223, 34, 54},
						Secret: []byte("b"),
					},
				},
			}
			return executeStatementResp, nil
		}
		var numModRows int64 = 2

		getOperationStatus := func(ctx context.Context, req *cli_service.TGetOperationStatusReq) (r *cli_service.TGetOperationStatusResp, err error) {
			getOperationStatusCount++
			getOperationStatusResp := &cli_service.TGetOperationStatusResp{
				OperationState:  cli_service.TOperationStatePtr(cli_service.TOperationState_FINISHED_STATE),
				NumModifiedRows: &numModRows,
			}
			return getOperationStatusResp, nil
		}

		testClient := &client.TestClient{
			FnExecuteStatement:   executeStatement,
			FnGetOperationStatus: getOperationStatus,
		}
		be := NewForTest(testClient, getTestSession(), config.WithDefaults())
		exStmtResp, opStatusResp, err := be.runQuery(context.Background(), backend.ExecRequest{Query: "select 1", Params: namedToParams([]driver.NamedValue{})})

		assert.NoError(t, err)
		assert.Equal(t, 1, executeStatementCount)
		assert.Equal(t, 1, getOperationStatusCount)
		assert.NotNil(t, exStmtResp)
		assert.NotNil(t, opStatusResp)
		assert.Equal(t, &numModRows, opStatusResp.NumModifiedRows)
	})

	t.Run("runQuery should return resp and error when query is canceled", func(t *testing.T) {
		var executeStatementCount, getOperationStatusCount int
		executeStatement := func(ctx context.Context, req *cli_service.TExecuteStatementReq) (r *cli_service.TExecuteStatementResp, err error) {
			executeStatementCount++
			executeStatementResp := &cli_service.TExecuteStatementResp{
				Status: &cli_service.TStatus{
					StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
				},
				OperationHandle: &cli_service.TOperationHandle{
					OperationId: &cli_service.THandleIdentifier{
						GUID:   []byte{1, 2, 3, 4, 2, 23, 4, 2, 3, 2, 4, 4, 223, 34, 23, 54},
						Secret: []byte("b"),
					},
				},
			}
			return executeStatementResp, nil
		}
		var numModRows int64 = 3

		getOperationStatus := func(ctx context.Context, req *cli_service.TGetOperationStatusReq) (r *cli_service.TGetOperationStatusResp, err error) {
			getOperationStatusCount++
			getOperationStatusResp := &cli_service.TGetOperationStatusResp{
				OperationState:  cli_service.TOperationStatePtr(cli_service.TOperationState_CANCELED_STATE),
				NumModifiedRows: &numModRows,
			}
			return getOperationStatusResp, nil
		}

		testClient := &client.TestClient{
			FnExecuteStatement:   executeStatement,
			FnGetOperationStatus: getOperationStatus,
		}
		be := NewForTest(testClient, getTestSession(), config.WithDefaults())
		exStmtResp, opStatusResp, err := be.runQuery(context.Background(), backend.ExecRequest{Query: "select 1", Params: namedToParams([]driver.NamedValue{})})

		assert.Error(t, err)
		assert.Equal(t, 1, executeStatementCount)
		assert.Equal(t, 1, getOperationStatusCount)
		assert.NotNil(t, exStmtResp)
		assert.NotNil(t, opStatusResp)
		assert.Equal(t, &numModRows, opStatusResp.NumModifiedRows)
	})

	t.Run("runQuery should return resp when query is finished with DirectResults", func(t *testing.T) {
		var executeStatementCount, getOperationStatusCount int
		executeStatement := func(ctx context.Context, req *cli_service.TExecuteStatementReq) (r *cli_service.TExecuteStatementResp, err error) {
			executeStatementCount++
			executeStatementResp := &cli_service.TExecuteStatementResp{
				Status: &cli_service.TStatus{
					StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
				},
				OperationHandle: &cli_service.TOperationHandle{
					OperationId: &cli_service.THandleIdentifier{
						GUID:   []byte{1, 2, 3, 4, 2, 23, 4, 2, 3, 2, 4, 4, 223, 34, 54, 87},
						Secret: []byte("b"),
					},
				},
				DirectResults: &cli_service.TSparkDirectResults{
					OperationStatus: &cli_service.TGetOperationStatusResp{
						Status: &cli_service.TStatus{
							StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
						},
						OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_FINISHED_STATE),
					},
				},
			}
			return executeStatementResp, nil
		}

		getOperationStatus := func(ctx context.Context, req *cli_service.TGetOperationStatusReq) (r *cli_service.TGetOperationStatusResp, err error) {
			getOperationStatusCount++
			getOperationStatusResp := &cli_service.TGetOperationStatusResp{
				OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_FINISHED_STATE),
			}
			return getOperationStatusResp, nil
		}

		testClient := &client.TestClient{
			FnExecuteStatement:   executeStatement,
			FnGetOperationStatus: getOperationStatus,
		}
		be := NewForTest(testClient, getTestSession(), config.WithDefaults())
		exStmtResp, opStatusResp, err := be.runQuery(context.Background(), backend.ExecRequest{Query: "select 1", Params: namedToParams([]driver.NamedValue{})})

		assert.NoError(t, err)
		assert.Equal(t, 1, executeStatementCount)
		assert.Equal(t, 0, getOperationStatusCount) // GetOperationStatus should not be called, already provided in DirectResults
		assert.NotNil(t, exStmtResp)
		assert.NotNil(t, opStatusResp)
	})

	t.Run("runQuery should return resp and err when query is cancelled with DirectResults", func(t *testing.T) {
		var executeStatementCount, getOperationStatusCount int
		executeStatement := func(ctx context.Context, req *cli_service.TExecuteStatementReq) (r *cli_service.TExecuteStatementResp, err error) {
			executeStatementCount++
			executeStatementResp := &cli_service.TExecuteStatementResp{
				Status: &cli_service.TStatus{
					StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
				},
				OperationHandle: &cli_service.TOperationHandle{
					OperationId: &cli_service.THandleIdentifier{
						GUID:   []byte{1, 2, 3, 4, 2, 23, 4, 2, 3, 2, 3, 4, 4, 223, 34, 54},
						Secret: []byte("b"),
					},
				},
				DirectResults: &cli_service.TSparkDirectResults{
					OperationStatus: &cli_service.TGetOperationStatusResp{
						Status: &cli_service.TStatus{
							StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
						},
						OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_CANCELED_STATE),
					},
				},
			}
			return executeStatementResp, nil
		}

		getOperationStatus := func(ctx context.Context, req *cli_service.TGetOperationStatusReq) (r *cli_service.TGetOperationStatusResp, err error) {
			getOperationStatusCount++
			getOperationStatusResp := &cli_service.TGetOperationStatusResp{
				OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_FINISHED_STATE),
			}
			return getOperationStatusResp, nil
		}

		testClient := &client.TestClient{
			FnExecuteStatement:   executeStatement,
			FnGetOperationStatus: getOperationStatus,
		}
		be := NewForTest(testClient, getTestSession(), config.WithDefaults())
		exStmtResp, opStatusResp, err := be.runQuery(context.Background(), backend.ExecRequest{Query: "select 1", Params: namedToParams([]driver.NamedValue{})})

		assert.Error(t, err)
		assert.Equal(t, 1, executeStatementCount)
		assert.Equal(t, 0, getOperationStatusCount) // GetOperationStatus should not be called, already provided in DirectResults
		assert.NotNil(t, exStmtResp)
		assert.NotNil(t, opStatusResp)
	})

	t.Run("runQuery should return resp when query is finished but DirectResults still live", func(t *testing.T) {
		var executeStatementCount, getOperationStatusCount int
		executeStatement := func(ctx context.Context, req *cli_service.TExecuteStatementReq) (r *cli_service.TExecuteStatementResp, err error) {
			executeStatementCount++
			executeStatementResp := &cli_service.TExecuteStatementResp{
				Status: &cli_service.TStatus{
					StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
				},
				OperationHandle: &cli_service.TOperationHandle{
					OperationId: &cli_service.THandleIdentifier{
						GUID:   []byte{1, 2, 3, 4, 2, 23, 4, 2, 3, 2, 3, 4, 4, 223, 34, 54},
						Secret: []byte("b"),
					},
				},
				DirectResults: &cli_service.TSparkDirectResults{
					OperationStatus: &cli_service.TGetOperationStatusResp{
						Status: &cli_service.TStatus{
							StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
						},
						OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_INITIALIZED_STATE),
					},
				},
			}
			return executeStatementResp, nil
		}
		var numModRows int64 = 3
		getOperationStatus := func(ctx context.Context, req *cli_service.TGetOperationStatusReq) (r *cli_service.TGetOperationStatusResp, err error) {
			getOperationStatusCount++
			getOperationStatusResp := &cli_service.TGetOperationStatusResp{
				OperationState:  cli_service.TOperationStatePtr(cli_service.TOperationState_FINISHED_STATE),
				NumModifiedRows: &numModRows,
			}
			return getOperationStatusResp, nil
		}

		testClient := &client.TestClient{
			FnExecuteStatement:   executeStatement,
			FnGetOperationStatus: getOperationStatus,
		}
		be := NewForTest(testClient, getTestSession(), config.WithDefaults())
		exStmtResp, opStatusResp, err := be.runQuery(context.Background(), backend.ExecRequest{Query: "select 1", Params: namedToParams([]driver.NamedValue{})})

		assert.NoError(t, err)
		assert.Equal(t, 1, executeStatementCount)
		assert.Equal(t, &numModRows, opStatusResp.NumModifiedRows)
		assert.Equal(t, 1, getOperationStatusCount)
		assert.NotNil(t, exStmtResp)
		assert.NotNil(t, opStatusResp)
	})

	t.Run("runQuery should return resp and err when query is cancelled after DirectResults still live", func(t *testing.T) {
		var executeStatementCount, getOperationStatusCount int
		executeStatement := func(ctx context.Context, req *cli_service.TExecuteStatementReq) (r *cli_service.TExecuteStatementResp, err error) {
			executeStatementCount++
			executeStatementResp := &cli_service.TExecuteStatementResp{
				Status: &cli_service.TStatus{
					StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
				},
				OperationHandle: &cli_service.TOperationHandle{
					OperationId: &cli_service.THandleIdentifier{
						GUID:   []byte{1, 2, 3, 4, 2, 23, 4, 2, 3, 2, 3, 4, 4, 223, 34, 54},
						Secret: []byte("b"),
					},
				},
				DirectResults: &cli_service.TSparkDirectResults{
					OperationStatus: &cli_service.TGetOperationStatusResp{
						Status: &cli_service.TStatus{
							StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
						},
						OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_INITIALIZED_STATE),
					},
				},
			}
			return executeStatementResp, nil
		}

		getOperationStatus := func(ctx context.Context, req *cli_service.TGetOperationStatusReq) (r *cli_service.TGetOperationStatusResp, err error) {
			getOperationStatusCount++
			getOperationStatusResp := &cli_service.TGetOperationStatusResp{
				OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_CANCELED_STATE),
			}
			return getOperationStatusResp, nil
		}

		testClient := &client.TestClient{
			FnExecuteStatement:   executeStatement,
			FnGetOperationStatus: getOperationStatus,
		}
		be := NewForTest(testClient, getTestSession(), config.WithDefaults())
		exStmtResp, opStatusResp, err := be.runQuery(context.Background(), backend.ExecRequest{Query: "select 1", Params: namedToParams([]driver.NamedValue{})})

		assert.Error(t, err)
		assert.Equal(t, 1, executeStatementCount)
		assert.Equal(t, 1, getOperationStatusCount)
		assert.NotNil(t, exStmtResp)
		assert.NotNil(t, opStatusResp)
	})
}

// TestBackend_Execute pins the load-bearing contract from backend.Backend.Execute:
// even when err is non-nil, the returned Operation MUST be non-nil so the caller
// can uniformly reach the operation-close and telemetry paths. This is what lets
// conn.runQuery use a single `if op == nil` guard to distinguish pre-backend
// failures (param conversion) from post-backend errors that still need close.
func TestBackend_Execute(t *testing.T) {
	// Case 1: the pre-handle error path — ExecuteStatement RPC itself fails.
	// The returned Operation has no handle, so its handle-less accessor contract
	// (StatementID "", Close closed=false with no RPC) must hold.
	t.Run("pre-handle failure returns non-nil Operation with handle-less accessors", func(t *testing.T) {
		closeCalls := 0
		be := NewForTest(
			&client.TestClient{
				FnExecuteStatement: func(ctx context.Context, req *cli_service.TExecuteStatementReq) (*cli_service.TExecuteStatementResp, error) {
					return nil, errors.New("boom: rpc failed before a handle was issued")
				},
				FnCloseOperation: func(ctx context.Context, req *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
					closeCalls++
					return &cli_service.TCloseOperationResp{}, nil
				},
			},
			getTestSession(),
			config.WithDefaults(),
		)
		op, err := be.Execute(context.Background(), backend.ExecRequest{Query: "select 1"})
		require.Error(t, err)
		require.NotNil(t, op, "Execute MUST return a non-nil Operation even on error")
		assert.Equal(t, "", op.StatementID(), "handle-less op returns empty statement id")
		closed, closeErr := op.Close(context.Background())
		assert.False(t, closed, "handle-less Close must not issue an RPC")
		assert.NoError(t, closeErr)
		assert.Equal(t, 0, closeCalls, "no CloseOperation RPC on a handle-less op")
	})

	// Case 2: the post-handle error path — the server returns a handle but the
	// operation lands in a non-FINISHED terminal state. The Operation must still
	// be non-nil (the caller uses it for Close + ExecutionError + telemetry) and
	// carry the handle so it can close the server op on the error path.
	t.Run("post-handle terminal-error returns non-nil Operation carrying handle+status", func(t *testing.T) {
		displayMsg := "server said no"
		closeCalls := 0
		be := NewForTest(
			&client.TestClient{
				FnExecuteStatement: func(ctx context.Context, req *cli_service.TExecuteStatementReq) (*cli_service.TExecuteStatementResp, error) {
					return &cli_service.TExecuteStatementResp{
						Status:          &cli_service.TStatus{StatusCode: cli_service.TStatusCode_SUCCESS_STATUS},
						OperationHandle: opHandle(),
						DirectResults: &cli_service.TSparkDirectResults{
							OperationStatus: &cli_service.TGetOperationStatusResp{
								OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_ERROR_STATE),
								DisplayMessage: &displayMsg,
							},
						},
					}, nil
				},
				FnCloseOperation: func(ctx context.Context, req *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
					closeCalls++
					return &cli_service.TCloseOperationResp{}, nil
				},
			},
			getTestSession(),
			config.WithDefaults(),
		)
		op, err := be.Execute(context.Background(), backend.ExecRequest{Query: "select 1"})
		require.Error(t, err)
		require.NotNil(t, op, "Execute MUST return a non-nil Operation even on error")
		assert.NotEmpty(t, op.StatementID(), "post-handle op carries a statement id")
		// The caller drives Close on the error path; it must issue the RPC because
		// the operation is still open on the server (ERROR_STATE, not CLOSED_STATE).
		closed, closeErr := op.Close(context.Background())
		assert.True(t, closed, "post-handle Close on a live ERROR_STATE op must issue the RPC")
		assert.NoError(t, closeErr)
		assert.Equal(t, 1, closeCalls)
		// ExecutionError must also work off the non-nil op and pull sqlstate from opStatusResp.
		require.Error(t, op.ExecutionError(context.Background(), err))
	})
}
