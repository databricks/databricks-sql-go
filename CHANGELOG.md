# Release History

## 0.1.x (Unreleased)

- Fix thread safety issue in connector

## 0.2.0 (2022-11-18)

- Support for DirectResults
- Support for context cancellation and timeout
- Session parameters (e.g.: timezone)
- Thrift Protocol update
- Several logging improvements
- Added better examples. See [workflow](https://github.com/databricks/databricks-sql-go/blob/main/examples/workflow/main.go)
- Added dbsql.NewConnector() function to help initialize DB
- Many other small improvements and bug fixes
- Removed support for client-side query parameterization
- Removed need to start DSN with "databricks://"

## 0.1.4 (2022-07-30)

- Fix: Could not fetch rowsets greater than the value of `maxRows` (#18)
- Updated default user agent
- Updated README and CONTRIBUTING

## 0.1.3 (2022-06-16)

- Add escaping of string parameters.

## 0.1.2 (2022-06-10)

- Fix timeout units to be milliseconds instead of nanos.

## 0.1.1 (2022-05-19)

- Fix module name

## 0.1.0 (2022-05-19)

- Initial release
