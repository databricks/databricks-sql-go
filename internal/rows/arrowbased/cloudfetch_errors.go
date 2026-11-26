package arrowbased

import (
	"context"
	stderrors "errors"
	"fmt"
	"regexp"
	"strings"

	dbsqlerr "github.com/databricks/databricks-sql-go/v2/errors"
	dbsqlerrint "github.com/databricks/databricks-sql-go/v2/internal/errors"
	"github.com/pkg/errors"
)

var errLinkExpired = stderrors.New(dbsqlerr.ErrLinkExpired)

var cloudFetchErrorURL = regexp.MustCompile(`(?i)(https?://|https?%3a%2f%2f)\S+`)

type cloudFetchLinkExpiredError struct {
	cause          error
	status         int
	renewalMessage string
}

func (expired *cloudFetchLinkExpiredError) Error() string {
	message := dbsqlerr.ErrLinkExpired
	if expired.status != 0 {
		message += fmt.Sprintf(": HTTP error %d", expired.status)
	} else if expired.cause != nil {
		message += ": " + redactCloudFetchRenewalError(expired.cause)
	}
	if expired.renewalMessage != "" {
		message += ": renewal failed: " + expired.renewalMessage
	}
	return message
}

func (expired *cloudFetchLinkExpiredError) Is(target error) bool {
	return target == errLinkExpired
}

func (expired *cloudFetchLinkExpiredError) Unwrap() error {
	return expired.cause
}

func combineCloudFetchErrors(first, second error) error {
	if first == nil || errors.Is(second, first) {
		return second
	}
	if second == nil || errors.Is(first, second) {
		return first
	}
	return stderrors.Join(first, second)
}

func redactCloudFetchRenewalError(err error) string {
	message := cloudFetchErrorURL.ReplaceAllString(err.Error(), "[redacted URL]")
	if len(message) > 512 {
		message = strings.ToValidUTF8(message[:512], "?") + "..."
	}
	return message
}

//go:noinline
func newCloudFetchExpiryError(ctx context.Context, cause error, status int, renewalFailure error) error {
	message := ""
	if renewalFailure != nil {
		message = redactCloudFetchRenewalError(renewalFailure)
		cause = combineCloudFetchErrors(cause, renewalFailure)
	}
	detail := errors.WithStack(&cloudFetchLinkExpiredError{cause: cause, status: status, renewalMessage: message})
	return dbsqlerrint.NewDriverError(ctx, errArrowRowsCloudFetchDownloadFailure, detail).WithCategory(dbsqlerrint.CategoryChunkDownload)
}

//go:noinline
func newCloudFetchDownloadError(ctx context.Context, cause error, status, retries int) error {
	message := ""
	if status != 0 {
		message = fmt.Sprintf("%s: HTTP error %d", errArrowRowsCloudFetchDownloadFailure, status)
		if retries >= 0 {
			message += fmt.Sprintf(" (after %d retries)", retries)
		}
	} else {
		message = fmt.Sprintf("%s: %v (after %d retries)", errArrowRowsCloudFetchDownloadFailure, cause, retries)
	}
	return dbsqlerrint.NewDriverError(ctx, message, cause).WithCategory(dbsqlerrint.CategoryChunkDownload)
}
