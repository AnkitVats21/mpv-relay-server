# MPV Relay Backend Makefile
# Uses pure-Go SQLite (modernc.org/sqlite) so cross-compilation does not require CGO.

BINARY_NAME=mpv-relay
BUILD_DIR=build
CMD_PATH=./cmd/relay

# Go build flags
LDFLAGS=-ldflags="-w -s"
CGO_ENABLED=0

.PHONY: all build run clean help build-all \
	build-linux-amd64 build-linux-arm64 build-linux-arm \
	build-darwin-amd64 build-darwin-arm64 \
	build-windows-amd64

all: build

help:
	@echo "Available commands:"
	@echo "  make build               Build binary for the current host platform"
	@echo "  make run                 Run the application locally"
	@echo "  make clean               Remove build artifacts"
	@echo "  make build-all           Cross-compile binaries for all supported platforms"
	@echo "  make build-linux-amd64   Build for Linux x86_64"
	@echo "  make build-linux-arm64   Build for Linux ARM64 (e.g. Raspberry Pi 4/5 64-bit)"
	@echo "  make build-linux-arm     Build for Linux ARMv7 (e.g. Raspberry Pi 3/4 32-bit)"
	@echo "  make build-darwin-amd64  Build for macOS Intel"
	@echo "  make build-darwin-arm64  Build for macOS Apple Silicon"
	@echo "  make build-windows-amd64 Build for Windows x86_64"

build:
	@echo "Building for host platform..."
	@mkdir -p $(BUILD_DIR)
	CGO_ENABLED=$(CGO_ENABLED) go build $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME) $(CMD_PATH)
	@echo "Built successfully: $(BUILD_DIR)/$(BINARY_NAME)"

run:
	CGO_ENABLED=$(CGO_ENABLED) go run $(CMD_PATH)

clean:
	@echo "Cleaning build directory..."
	rm -rf $(BUILD_DIR)
	@echo "Cleaned."

# ── Cross Compilation ──────────────────────────────────────────────────────────

build-all: build-linux-amd64 build-linux-arm64 build-linux-arm build-darwin-amd64 build-darwin-arm64 build-windows-amd64
	@echo "All platforms built successfully!"

build-linux-amd64:
	@echo "Building for Linux amd64..."
	@mkdir -p $(BUILD_DIR)
	GOOS=linux GOARCH=amd64 CGO_ENABLED=$(CGO_ENABLED) go build $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME)-linux-amd64 $(CMD_PATH)

build-linux-arm64:
	@echo "Building for Linux arm64 (Raspberry Pi 64-bit)..."
	@mkdir -p $(BUILD_DIR)
	GOOS=linux GOARCH=arm64 CGO_ENABLED=$(CGO_ENABLED) go build $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME)-linux-arm64 $(CMD_PATH)

build-linux-arm:
	@echo "Building for Linux armv7 (Raspberry Pi 32-bit)..."
	@mkdir -p $(BUILD_DIR)
	GOOS=linux GOARCH=arm GOARM=7 CGO_ENABLED=$(CGO_ENABLED) go build $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME)-linux-arm $(CMD_PATH)

build-darwin-amd64:
	@echo "Building for macOS amd64 (Intel)..."
	@mkdir -p $(BUILD_DIR)
	GOOS=darwin GOARCH=amd64 CGO_ENABLED=$(CGO_ENABLED) go build $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME)-darwin-amd64 $(CMD_PATH)

build-darwin-arm64:
	@echo "Building for macOS arm64 (Apple Silicon)..."
	@mkdir -p $(BUILD_DIR)
	GOOS=darwin GOARCH=arm64 CGO_ENABLED=$(CGO_ENABLED) go build $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME)-darwin-arm64 $(CMD_PATH)

build-windows-amd64:
	@echo "Building for Windows amd64..."
	@mkdir -p $(BUILD_DIR)
	GOOS=windows GOARCH=amd64 CGO_ENABLED=$(CGO_ENABLED) go build $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME)-windows-amd64.exe $(CMD_PATH)
