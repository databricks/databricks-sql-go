module github.com/databricks/databricks-sql-go

go 1.27.3

tool gotest.tools/gotestsum

require (
	github.com/apache/arrow-go/v18 v18.8.0
	github.com/apache/thrift v0.24.0
	github.com/coreos/go-oidc/v3 v3.21.0
	github.com/golang-jwt/jwt/v5 v5.2.2
	github.com/joho/godotenv v1.4.0
	github.com/mattn/go-isatty v0.0.24
	github.com/pierrec/lz4/v4 v4.1.29
	github.com/stretchr/testify v1.12.1
	golang.org/x/oauth2 v0.36.0
	golang.org/x/sync v0.22.0
)

require (
	github.com/andybalholm/brotli v1.2.7 // indirect
	github.com/dnephin/pflag v1.0.7 // indirect
	github.com/fatih/color v1.16.0 // indirect
	github.com/fsnotify/fsnotify v1.5.4 // indirect
	github.com/go-jose/go-jose/v4 v4.1.6 // indirect
	github.com/goccy/go-json v0.10.6 // indirect
	github.com/google/flatbuffers v25.12.19+incompatible // indirect
	github.com/google/shlex v0.0.0-20191202100458-e7afc7fbc510 // indirect
	github.com/hashicorp/go-cleanhttp v0.5.2 // indirect
	github.com/klauspost/compress v1.20.2 // indirect
	github.com/klauspost/cpuid/v2 v2.4.0 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	// Deliberate security-scan minimum; go mod tidy removes this unused requirement.
	golang.org/x/crypto v0.55.0 // indirect
	golang.org/x/exp v0.0.0-20260112195511-716be5621a96 // indirect
	golang.org/x/mod v0.40.0 // indirect
	golang.org/x/telemetry v0.0.0-20260811182544-a038080d80e5 // indirect
	golang.org/x/term v0.45.0 // indirect
	golang.org/x/tools v0.49.0 // indirect
	gotest.tools/gotestsum v1.8.2 // indirect
)

require (
	github.com/hashicorp/go-retryablehttp v0.7.7
	github.com/pkg/browser v0.0.0-20210911075715-681adbf594b8
	github.com/pkg/errors v0.9.1
	github.com/rs/zerolog v1.28.0
	golang.org/x/sys v0.47.0 // indirect
)

// Per-platform kernel library modules, distributed by the separate
// github.com/databricks/databricks-sql-kernel-bindings repo (one nested Go
// module per platform, lib/<os>_<arch>). Each carries that platform's prebuilt
// kernel static archive + its cgo link directive; a kernel build downloads only
// the archive for the platform it targets (build-tag gated), and a pure-Go
// Thrift build downloads none of them.
//
// The versions are pinned in lockstep with the driver release. A consumer's
// `go get github.com/databricks/databricks-sql-go@vX.Y.Z` transitively pins the
// matching per-platform kernel archive; upgrading the driver moves the kernel
// version. See the databricks-sql-kernel-bindings repo README.
require (
	github.com/databricks/databricks-sql-kernel-bindings/lib/darwin_amd64 v1.4.0
	github.com/databricks/databricks-sql-kernel-bindings/lib/darwin_arm64 v1.4.0
	github.com/databricks/databricks-sql-kernel-bindings/lib/linux_amd64 v1.4.0
	github.com/databricks/databricks-sql-kernel-bindings/lib/linux_arm v1.4.0
	github.com/databricks/databricks-sql-kernel-bindings/lib/linux_arm64 v1.4.0
	github.com/databricks/databricks-sql-kernel-bindings/lib/windows_amd64 v1.4.0
	github.com/databricks/databricks-sql-kernel-bindings/lib/windows_arm64 v1.4.0
)
