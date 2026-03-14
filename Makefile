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