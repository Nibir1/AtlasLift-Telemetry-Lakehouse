# Purpose: Cross-Platform Workflow automation for AtlasLift-Telemetry-Lakehouse
# Author: Lead Cloud Data Architect
# Date: 2026-03-15

.PHONY: help clean lock install test-harness infra-up infra-down generate-env bundle-validate run-pipeline

RESOURCE_GROUP := atlaslift-rg
LOCATION := swedencentral
WORKSPACE_NAME := atlaslift-dbw-dev

help:
	@echo "AtlasLift Telemetry Lakehouse - Command Interface"
	@echo "-------------------------------------------------"
	@echo "make clean        : Remove corrupted lock files and .venv"
	@echo "make lock         : Generate native TOML uv.lock using pyproject.toml"
	@echo "make install      : Create .venv and install dependencies (including dev tools)"
	@echo "make test-harness : Verify the local PySpark Delta testing harness"
	@echo ""
	@echo "Infrastructure:"
	@echo "make infra-up         : Deploy Azure + Databricks workspace"
	@echo "make infra-down       : Destroy Azure resources"
	@echo "make generate-env     : Generate cross-platform .env file via Python"
	@echo "make bundle-validate  : Validate Databricks bundle via Python dotenv wrapper"
	@echo "make run-pipeline     : Execute Medallion Pipeline via Python dotenv wrapper"

clean:
	@echo "Cleaning up environment files..."
	uv cache clean
	-rm -rf .venv uv.lock

lock:
	@echo "Generating native TOML uv.lock..."
	uv lock

install:
	@echo "Syncing virtual environment with lock file..."
	uv sync --extra dev

test-harness:
	@echo "Booting local PySpark test harness..."
	uv run pytest tests/ -v --setup-show

infra-up:
	@echo "Deploying Azure Resource Group and Databricks Workspace via Bicep..."
	az group create --name $(RESOURCE_GROUP) --location $(LOCATION) -o none
	az deployment group create --resource-group $(RESOURCE_GROUP) --template-file infra/workspace.bicep --parameters workspaceName=$(WORKSPACE_NAME) -o table
	@echo "Cloud infrastructure deployed successfully."

infra-down:
	@echo "Destroying all Azure resources to save credits..."
	az group delete --name $(RESOURCE_GROUP) --yes --no-wait
	@echo "Deletion initiated. This happens asynchronously in the background."

generate-env:
	@echo "Generating cross-platform .env file..."
	uv run python scripts/generate_env.py

bundle-validate:
	@echo "Validating Databricks Asset Bundle against live workspace..."
	uv run python -c "import subprocess, sys; from dotenv import load_dotenv; load_dotenv(); sys.exit(subprocess.run('databricks bundle validate', shell=True).returncode)"

run-pipeline:
	@echo "Executing Medallion Pipeline on Azure Databricks..."
	uv run python -c "import subprocess, sys; from dotenv import load_dotenv; load_dotenv(); sys.exit(subprocess.run('databricks bundle run atlaslift_medallion_pipeline -t prod', shell=True).returncode)"