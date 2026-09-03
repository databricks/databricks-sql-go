BINARY         = databricks-sql-go
PACKAGE        = github.com/databricks/$(BINARY)
VER_PREFIX     = $(PACKAGE)/version
PRODUCTION     ?= false
DATE           = $(shell date "+%Y-%m-%d")
GIT_BRANCH     = $(shell git rev-parse --abbrev-ref 2>/dev/null)
GIT_COMMIT     = $(shell git rev-parse HEAD 2>/dev/null)
GIT_TAG        = $(shell if [ -z "`git status --porcelain`" ]; then git describe --exact-match --tags HEAD 2>/dev/null; fi)
VERSIONREL     = $(shell if [ -z "`git status --porcelain`" ]; then git rev-parse --short HEAD 2>/dev/null ; else echo "dirty"; fi)
PKGS           = $(shell go list ./... | grep -v /vendor)
LDFLAGS        = -X $(VER_PREFIX).Version=$(VERSIONREL) -X $(VER_PREFIX).Revision=$(GIT_COMMIT) -X $(VER_PREFIX).Branch=$(GIT_BRANCH) -X $(VER_PREFIX).BuildUser=$(shell whoami)  -X $(VER_PREFIX).BuildDate=$(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
GOBUILD_ARGS   =
GO             = CGO_ENABLED=0 go
PLATFORMS      = windows linux darwin
os             = $(word 1, $@)

TEST_RESULTS_DIR ?= ./test-results


ifneq (${GIT_TAG},)
override DOCKER_TAG = ${GIT_TAG}
override VERSIONREL = ${GIT_TAG}
endif

ifeq (${PRODUCTION}, true)
LDFLAGS += -w -s -extldflags "-static"
GOBUILD_ARGS += -v
endif

.PHONY: help
help:  ## Show this help.
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {sub("\\\\n",sprintf("\n%22c"," "), $$2);printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: all
all: gen fmt lint test coverage  ## format and test everything

bin/golangci-lint: go.mod go.sum
	GOBIN=$(CURDIR)/bin go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2

bin/gotestsum: go.mod go.sum
	@mkdir -p bin/
	$(GO) build -o bin/gotestsum gotest.tools/gotestsum

.PHONY: tools
tools: bin/golangci-lint bin/gotestsum  ## Build the development tools

.PHONY: fmt
fmt:  ## Format the go code.
	gofmt -w -s .

.PHONY: lint
lint: bin/golangci-lint  ## Lint the go code to ensure code sanity.
	./bin/golangci-lint run

.PHONY: test
test: bin/gotestsum  ## Run the go unit tests.
	@echo "INFO: Running all go unit tests."
	CGO_ENABLED=0 ./bin/gotestsum --format pkgname-and-test-fails --junitfile $(TEST_RESULTS_DIR)/unit-tests.xml ./...

.PHONY: test-race
test-race:
	@echo "INFO: Running all go unit tests checking for race conditions."
	go test -race


.PHONY: coverage
coverage: bin/gotestsum  ## Report the unit test code coverage.
	@echo "INFO: Generating unit test coverage report."
	CGO_ENABLED=0 ./bin/gotestsum --format pkgname-and-test-fails -- -coverprofile=$(TEST_RESULTS_DIR)/coverage.txt -covermode=atomic ./...
	go tool cover -html=$(TEST_RESULTS_DIR)/coverage.txt -o $(TEST_RESULTS_DIR)/coverage.html

.PHONY: sec
sec: lint  ## Run the snyk vulnerability scans.
	@echo "INFO: Running go vulnerability scans."
	snyk test .

.PHONY: build
build: linux darwin  ## Build the multi-arch binaries

.PHONY: $(PLATFORMS)
$(PLATFORMS):
	mkdir -p bin
	GOOS=$(os) GOARCH=amd64 $(GO) build $(GOBUILD_ARGS) -ldflags '$(LDFLAGS)' -o bin/$(BINARY)-$(os)-amd64 .

# ── Kernel (SEA) backend ──────────────────────────────────────────────────────
# Opt-in cgo build that links the Rust SQL kernel's C ABI, gated behind the
# `databricks_kernel` build tag. These targets are wholly separate from the
# pure-Go defaults above (which stay CGO_ENABLED=0 and never touch the kernel).
KERNEL_REV        = $(shell cat KERNEL_REV)
KERNEL_REPO      ?= https://github.com/databricks/databricks-sql-kernel.git
KERNEL_SRC       ?= build/kernel-src
# Target OS/arch (honors GOOS/GOARCH overrides) vs the actual host, so the build
# step can reject a source cross-build that would drop a host .a into a foreign
# target dir. Multi-OS support is via native per-OS runners (host == target) or
# staging a prebuilt .a with KERNEL_LOCAL_A — not cross-compiling from source.
KERNEL_GOOS       = $(shell go env GOOS)
KERNEL_GOARCH     = $(shell go env GOARCH)
KERNEL_GOHOSTOS   = $(shell go env GOHOSTOS)
KERNEL_GOHOSTARCH = $(shell go env GOHOSTARCH)
KERNEL_LIB_DIR    = internal/backend/kernel/lib/$(KERNEL_GOOS)_$(KERNEL_GOARCH)
KERNEL_INC_DIR    = internal/backend/kernel/include
KERNEL_GO         = CGO_ENABLED=1 go
KERNEL_TAGS       = -tags databricks_kernel
KERNEL_LOCAL_LDFLAGS = -L$(abspath $(KERNEL_LIB_DIR)) -l:libdatabricks_sql_kernel.a

# SHIPPED PATH (consumers): the kernel archives are NOT built here. They come from
# the external per-platform modules in github.com/databricks/databricks-sql-kernel-bindings
# that this repo requires in go.mod; a kernel opt-in `go get` build downloads only
# the target platform's archive (no Rust, no build step). The targets below are
# for LOCAL DEVELOPMENT against a kernel source checkout only.
#
# Local-dev flow: `make test-kernel` builds the host-platform archive from the
# pinned KERNEL_REV and puts it before the published archive on the linker line.
.PHONY: kernel-lib
kernel-lib:  ## Build the pinned kernel static lib + header locally (source build, dev only).
	KERNEL_REPO="$(KERNEL_REPO)" KERNEL_REV="$(KERNEL_REV)" KERNEL_SRC="$(KERNEL_SRC)" \
	KERNEL_LIB_DIR="$(KERNEL_LIB_DIR)" KERNEL_INC_DIR="$(KERNEL_INC_DIR)" \
	KERNEL_GOOS="$(KERNEL_GOOS)" KERNEL_GOARCH="$(KERNEL_GOARCH)" \
	KERNEL_GOHOSTOS="$(KERNEL_GOHOSTOS)" KERNEL_GOHOSTARCH="$(KERNEL_GOHOSTARCH)" \
	KERNEL_LOCAL_A="$(KERNEL_LOCAL_A)" KERNEL_LOCAL_HEADER="$(KERNEL_LOCAL_HEADER)" \
	./build/kernel-lib.sh

.PHONY: build-kernel
build-kernel: kernel-lib  ## Build the driver against the source-built KERNEL_REV archive.
	CGO_LDFLAGS='$(KERNEL_LOCAL_LDFLAGS) $(CGO_LDFLAGS)' $(KERNEL_GO) build $(KERNEL_TAGS) ./...

.PHONY: test-kernel
test-kernel: kernel-lib  ## Run kernel-tagged tests against the source-built KERNEL_REV archive.
	CGO_LDFLAGS='$(KERNEL_LOCAL_LDFLAGS) $(CGO_LDFLAGS)' $(KERNEL_GO) test $(KERNEL_TAGS) ./...
