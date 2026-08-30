.PHONY: all test lint tidy build help
.DEFAULT_GOAL := help

SERVICES := brain scheduler-mcp discord-mcp dashboard

help: ## Display available targets
	@echo "Aerial Monorepo Development Commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

test: ## Run unit tests across all Go microservices
	@for svc in $(SERVICES); do \
		echo "=== Testing $$svc ==="; \
		(cd $$svc && go test -v ./...) || exit 1; \
	done

lint: ## Run golangci-lint across all Go microservices
	@for svc in $(SERVICES); do \
		echo "=== Linting $$svc ==="; \
		(cd $$svc && golangci-lint run ./...) || exit 1; \
	done

tidy: ## Run go mod tidy across all Go microservices
	@for svc in $(SERVICES); do \
		echo "=== Tidying $$svc ==="; \
		(cd $$svc && go mod tidy) || exit 1; \
	done

build: ## Build all Go service binaries locally
	@for svc in $(SERVICES); do \
		echo "=== Building $$svc ==="; \
		(cd $$svc && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" ./...) || exit 1; \
	done
