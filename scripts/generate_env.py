"""
Purpose: Cross-platform environment variable generator.
Author: Nahasat Nibir
Date: 2026-03-15
"""

import subprocess
import time
import sys

def get_cmd_output(cmd: str) -> str:
    """Executes a shell command and returns the string output."""
    try:
        # shell=True is required for Windows compatibility with az CLI
        return subprocess.check_output(cmd, shell=True, text=True).strip()
    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {cmd}\n{e}")
        sys.exit(1)

print("Pulling Azure context and live Databricks URL via Python...")

tenant_id = get_cmd_output("az account show --query tenantId -o tsv")
sub_id = get_cmd_output("az account show --query id -o tsv")
db_host = get_cmd_output("az databricks workspace show --resource-group atlaslift-rg --name atlaslift-dbw-dev --query workspaceUrl -o tsv")
storage_name = f"atlasliftlake{int(time.time())}"

env_content = f"""# Auto-generated .env file for AtlasLift-Telemetry-Lakehouse
ENVIRONMENT=dev
LOG_LEVEL=INFO
AZURE_TENANT_ID={tenant_id}
AZURE_SUBSCRIPTION_ID={sub_id}
STORAGE_ACCOUNT_NAME={storage_name}
STORAGE_CONTAINER_NAME=telemetry-raw
DATABRICKS_HOST=https://{db_host}
DATABRICKS_AUTH_TYPE=azure-cli
"""

with open(".env", "w") as f:
    f.write(env_content)

print("✅ .env file successfully synchronized with live Azure infrastructure!")