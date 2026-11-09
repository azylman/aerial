.PHONY: all test lint tidy build help console-ui
.DEFAULT_GOAL := help

SERVICES := services/eri services/haro mcp/discord-mcp mcp/kernel-mcp services/console services/coven services/vanadis services/synapse services/prospera pkg

help: ## Display available targets
	@echo "Aerial Monorepo Development Commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

test: ## Run unit tests across all Go microservices and Frontend
	@for svc in $(SERVICES); do \
		echo "=== Testing $$svc ==="; \
		(cd $$svc && go test -v ./...) || exit 1; \
	done
	@if command -v node >/dev/null 2>&1 && [ -f "services/console/app.test.js" ]; then \
		echo "=== Testing Permet HUD Frontend ==="; \
		node --test services/console/*.test.js || exit 1; \
	fi
	@$(MAKE) --no-print-directory console-ui

lint: ## Run golangci-lint across all Go microservices and Frontend
	@for svc in $(SERVICES); do \
		echo "=== Linting $$svc ==="; \
		(cd $$svc && golangci-lint run ./...) || exit 1; \
	done
	@if command -v node >/dev/null 2>&1 && [ -f "services/console/static/app.js" ]; then \
		echo "=== Linting Permet HUD Frontend Syntax ==="; \
		node --check services/console/static/app.js || exit 1; \
	fi
	@if command -v node >/dev/null 2>&1 && [ -f "services/docs-service/app/assets/js/plugins/docsify-mermaid-cyberpunk.js" ]; then \
		echo "=== Linting Docsify Custom Plugin Syntax ==="; \
		node --check services/docs-service/app/assets/js/plugins/docsify-mermaid-cyberpunk.js || exit 1; \
	fi

CONSOLE_UI := services/console/ui

console-ui: ## Typecheck, coverage-test and build the React console UI (services/console/ui)
	@if command -v npm >/dev/null 2>&1 && [ -f "$(CONSOLE_UI)/package.json" ]; then \
		echo "=== Testing Console React UI ==="; \
		cd $(CONSOLE_UI) && \
		{ { [ -d node_modules ] && [ ! package-lock.json -nt node_modules/.package-lock.json ]; } || npm ci --no-audit --no-fund; } && \
		npm run --silent typecheck && \
		npm run --silent test:coverage && \
		npm run --silent build || exit 1; \
	fi

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

setup-hooks: ## Configure git to use monorepo .githooks directory
	@git config core.hooksPath .githooks
	@echo "Configured git core.hooksPath -> .githooks"

