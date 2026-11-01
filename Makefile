VERSION ?= $(patsubst v%,%,$(shell git describe --tags --always --dirty 2>/dev/null || echo dev))
LDFLAGS := -X main.version=$(VERSION)

.DEFAULT_GOAL := build
.PHONY: build test install redeploy check lint fmt vet dist

# go.mod pins sock-contract and sock-client by tag; the Go proxy fetches them.
# To build against local checkouts instead, use an untracked go.work (gitignored).

build:
	go build -ldflags "$(LDFLAGS)" -o bin/sock-mcp ./cmd/sock-mcp

test:
	go test -race ./...

install:
	go install -ldflags "$(LDFLAGS)" ./cmd/sock-mcp

redeploy:
	@echo "sock-mcp has nothing to deploy; run 'make install' to update the local binary"

lint:
	golangci-lint run

fmt:
	@out="$$(gofmt -l .)"; if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

# Windows too: sock-mcp ships a Windows build.
vet:
	go vet ./...
	GOOS=windows go vet ./...

check: fmt vet lint test

# dist cross-builds the release archives publish-release.sh uploads, one per
# platform, all at once: sock-mcp-<os>-<arch>.tar.gz (.zip on Windows) holding the
# binary and LICENSE at the archive root. The release workflow runs this.
DIST_PLATFORMS := darwin-arm64 darwin-amd64 linux-arm64 linux-amd64 windows-amd64

dist:
	rm -rf bin/dist bin/dist-stage
	$(MAKE) -j$(words $(DIST_PLATFORMS)) $(addprefix dist-,$(DIST_PLATFORMS)) VERSION=$(VERSION)
	rm -rf bin/dist-stage

# Inside dist-%: $* is <os>-<arch>.
dist_os = $(word 1,$(subst -, ,$*))
dist_exe = sock-mcp$(if $(filter windows,$(dist_os)),.exe)

dist-%:
	mkdir -p bin/dist bin/dist-stage/$*
	CGO_ENABLED=0 GOOS=$(dist_os) GOARCH=$(word 2,$(subst -, ,$*)) go build -trimpath -ldflags "$(LDFLAGS)" -o bin/dist-stage/$*/$(dist_exe) ./cmd/sock-mcp
	cp LICENSE bin/dist-stage/$*/
	$(if $(filter windows,$(dist_os)),cd bin/dist-stage/$* && zip -qX ../../dist/sock-mcp-$*.zip $(dist_exe) LICENSE,COPYFILE_DISABLE=1 tar -czf bin/dist/sock-mcp-$*.tar.gz -C bin/dist-stage/$* $(dist_exe) LICENSE)
