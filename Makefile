# Purpose: Workflow automation for AtlasLift-Telemetry-Lakehouse
# Author: Lead Cloud Data Architect
# Date: 2026-03-14

.PHONY: help clean lock install test-harness

help:
	@echo "AtlasLift Telemetry Lakehouse - Command Interface"
	@echo "-------------------------------------------------"
	@echo "make clean        : Remove corrupted lock files and .venv"
	@echo "make lock         : Generate native TOML uv.lock using pyproject.toml"
	@echo "make install      : Create .venv and install dependencies (including dev tools)"
	@echo "make test-harness : Verify the local PySpark Delta testing harness"

clean:
	@echo "Cleaning up corrupted environment files..."
	rm -rf .venv uv.lock

lock:
	@echo "Generating native TOML uv.lock..."
	uv lock

install:
	@echo "Syncing virtual environment with lock file (including dev tools)..."
	uv sync --extra dev

test-harness:
	@echo "Booting local PySpark test harness..."
	uv run pytest tests/ -v --setup-show

# --- Infrastructure & Environment Automation ---

# Variables for Azure Deployment
RESOURCE_GROUP := atlaslift-rg
LOCATION := swedencentral
WORKSPACE_NAME := atlaslift-dbw-dev

.PHONY: infra-up infra-down generate-env bundle-validate

infra-up:
	@echo "Deploying Azure Resource Group and Databricks Workspace via Bicep..."
	@az group create --name $(RESOURCE_GROUP) --location $(LOCATION) -o none
	@az deployment group create \
		--resource-group $(RESOURCE_GROUP) \
		--template-file infra/workspace.bicep \
		--parameters workspaceName=$(WORKSPACE_NAME) \
		-o table
	@echo "✅ Cloud infrastructure deployed successfully."

infra-down:
	@echo "🚨 Destroying all Azure resources to save credits..."
	@az group delete --name $(RESOURCE_GROUP) --yes --no-wait
	@echo "Deletion initiated. This happens asynchronously in the background."

generate-env:
	@echo "Pulling Azure context and live Databricks URL..."
	@echo "# Auto-generated .env file for AtlasLift-Telemetry-Lakehouse" > .env
	@echo "ENVIRONMENT=dev" >> .env
	@echo "LOG_LEVEL=INFO" >> .env
	@echo "AZURE_TENANT_ID=$$(az account show --query tenantId -o tsv)" >> .env
	@echo "AZURE_SUBSCRIPTION_ID=$$(az account show --query id -o tsv)" >> .env
	@echo "STORAGE_ACCOUNT_NAME=atlasliftlake$$(date +%s)" >> .env
	@echo "STORAGE_CONTAINER_NAME=telemetry-raw" >> .env
	@echo "DATABRICKS_HOST=https://$$(az databricks workspace show --resource-group $(RESOURCE_GROUP) --name $(WORKSPACE_NAME) --query workspaceUrl -o tsv)" >> .env
	@echo "DATABRICKS_AUTH_TYPE=azure-cli" >> .env
	@echo "✅ .env file successfully synchronized with live Azure infrastructure!"
	@cat .env

bundle-validate:
	@echo "Validating Databricks Asset Bundle against live workspace..."
	@export $$(grep -v '^#' .env | xargs) && databricks bundle validate