package client

import (
	"bytes"
	"io"
	"net/http"
	"time"

	"github.com/databricks/databricks-sql-go/logger"
)

type retryTransport struct {
	client       *http.Client
	retryWaitMin time.Duration
	retryWaitMax time.Duration
	retryMax     int
}

func (transport *retryTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	var getBody func() (io.ReadCloser, error)
	contentLength := request.ContentLength
	if request.Body != nil {
		defer request.Body.Close() //nolint:errcheck
		getBody = request.GetBody
		if getBody == nil {
			body, err := io.ReadAll(request.Body)
			if err != nil {
				return nil, err
			}
			if len(body) == 0 {
				contentLength = 0
			}
			getBody = func() (io.ReadCloser, error) {
				if len(body) == 0 {
					return http.NoBody, nil
				}
				return io.NopCloser(bytes.NewReader(body)), nil
			}
		}
	}

	logger.Debug().Msg("performing request")
	for attempt := 0; ; attempt++ {
		attemptRequest := request.Clone(request.Context())
		attemptRequest.ContentLength = contentLength
		if getBody != nil {
			body, err := getBody()
			if err != nil {
				transport.client.CloseIdleConnections()
				return nil, err
			}
			attemptRequest.Body = body
		}

		response, doErr := transport.client.Do(attemptRequest)
		retry, checkErr := RetryPolicy(request.Context(), response, doErr)
		if doErr != nil {
			logger.Error().Msg("request failed")
		}
		if !retry && doErr == nil && checkErr == nil {
			return response, nil
		}
		if !retry || attempt >= transport.retryMax {
			if checkErr == nil {
				checkErr = doErr
			}
			_, err := errorHandler(response, checkErr, attempt+1)
			if response != nil {
				_ = response.Body.Close()
			}
			transport.client.CloseIdleConnections()
			return nil, err
		}

		if response != nil {
			if doErr == nil {
				if _, err := io.Copy(io.Discard, io.LimitReader(response.Body, 4096)); err != nil {
					logger.Error().Msg("error reading response body")
				}
			}
			_ = response.Body.Close()
		}
		wait := backoff(transport.retryWaitMin, transport.retryWaitMax, attempt, response)
		logger.Debug().Msg("retrying request")
		timer := time.NewTimer(wait)
		select {
		case <-request.Context().Done():
			timer.Stop()
			transport.client.CloseIdleConnections()
			return nil, request.Context().Err()
		case <-timer.C:
		}
	}
}
