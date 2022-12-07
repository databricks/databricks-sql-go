/*
Package dbsql implements the go driver to Databricks SQL

# Usage

Clients should use the database/sql package in conjunction with the driver:

	import (
		"database/sql"

		_ "github.com/databricks/databricks-sql-go"
	)

	func main() {
		db, err := sql.Open("databricks", "token:<token>@<hostname>:<port>/<endpoint_path>")

		if err != nil {
			log.Fatal(err)
		}
		defer db.Close()
	}

# Connection via DSN (Data Source Name)

Use sql.Open() to create a database handle via a data source name string:

	db, err := sql.Open("databricks", "<dsn_string>")

The DSN format is:

	token:[my_token]@[hostname]:[port]/[endpoint http path]?param=value

Supported optional connection parameters can be specified in param=value and include:

  - catalog: Sets the initial catalog name in the session
  - schema: Sets the initial schema name in the session
  - maxRows: Sets up the max rows fetched per request. Default is 10000
  - timeout: Adds timeout (in seconds) for the server query execution. Default is no timeout
  - userAgentEntry: Used to identify partners. Set as a string with format <isv-name+product-name>

Supported optional session parameters can be specified in param=value and include:

  - ansi_mode: (Boolean string). Session statements will adhere to rules defined by ANSI SQL specification. Default is "false"
  - timezone: (e.g. "America/Los_Angeles"). Sets the timezone of the session

# Connection via new connector object

Use sql.OpenDB() to create a database handle via a new connector object created with dbsql.NewConnector():

	import (
		"database/sql"
		dbsql "github.com/databricks/databricks-sql-go"
	)

	func main() {
		connector, err := dbsql.NewConnector(
			dbsql.WithServerHostname(<hostname>),
			dbsql.WithPort(<port>),
			dbsql.WithHTTPPath(<http_path>),
			dbsql.WithAccessToken(<my_token>)
		)
		if err != nil {
			log.Fatal(err)
		}
		defer db.close()
	}

	db := sql.OpenDB(connector)

Supported functional options include:

  - WithServerHostname(<hostname> string): Sets up the server hostname. Mandatory
  - WithPort(<port> int): Sets up the server port. Mandatory
  - WithAccessToken(<my_token> string): Sets up the Personal Access Token. Mandatory
  - WithHTTPPath(<http_path> string): Sets up the endpoint to the warehouse. Mandatory
  - WithInitialNamespace(<catalog> string, <schema> string): Sets up the catalog and schema name in the session. Optional
  - WithMaxRows(<max_rows> int): Sets up the max rows fetched per request. Default is 10000. Optional
  - WithSessionParams(<params_map> map[string]string): Sets up session parameters including "timezone" and "ansi_mode". Optional
  - WithTimeout(<timeout> Duration). Adds timeout (in time.Duration) for the server query execution. Default is no timeout. Optional
  - WithUserAgentEntry(<isv-name+product-name> string). Used to identify partners. Optional
*/
package dbsql
