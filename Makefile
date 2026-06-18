GO_TEST_FLAGS ?=
RUN ?=
DIR ?= ./...

# Tests

.PHONY: test
test: test-unit test-external

.PHONY: test-unit
test-unit:
	go test $(GO_TEST_FLAGS) ./core/

.PHONY: test-external
test-external:
	cd tests && go test $(GO_TEST_FLAGS) ./...

.PHONY: test-integration
test-integration:
	cd tests && go test $(GO_TEST_FLAGS) ./integration/

.PHONY: test-race
test-race:
	cd tests && go test -race $(GO_TEST_FLAGS) ./race/ ./regression/

.PHONY: test-regression
test-regression:
	cd tests && go test -race $(GO_TEST_FLAGS) ./regression/

.PHONY: test-stress
test-stress:
	cd tests && go test $(GO_TEST_FLAGS) -timeout 60s ./stress/

.PHONY: test-bench
test-bench:
	cd tests && go test -bench=. -benchmem $(GO_TEST_FLAGS) ./benchmark/

.PHONY: test-run
test-run:
	cd tests && go test $(GO_TEST_FLAGS) -run="$(RUN)" $(DIR)

# Code quality

.PHONY: vet
vet:
	go vet ./...

.PHONY: tidy
tidy:
	go mod tidy
	cd tests && go mod tidy

# Build

BINARY ?= etcdctl+
LDFLAGS := -s -w

.PHONY: build
build:
	go build -o bin/etcdctl+ main.go

.PHONY: build-all
build-all: build-mac build-linux ## 编译 mac + linux 多平台二进制

.PHONY: build-mac
build-mac: ## 编译 macOS (arm64 + amd64)
	GOOS=darwin GOARCH=arm64 go build -ldflags "$(LDFLAGS)" -o bin/$(BINARY)-darwin-arm64 main.go
	GOOS=darwin GOARCH=amd64 go build -ldflags "$(LDFLAGS)" -o bin/$(BINARY)-darwin-amd64 main.go

.PHONY: build-linux
build-linux: ## 编译 Linux amd64
	GOOS=linux GOARCH=amd64 go build -ldflags "$(LDFLAGS)" -o bin/$(BINARY)-linux-amd64 main.go
