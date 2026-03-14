// Purpose: Production IaC for Azure Databricks Workspace
// Author: Nahasat Nibir
// Date: 2026-03-14

@description('The name of the Azure Databricks workspace to create.')
param workspaceName string

@description('Location for all resources.')
param location string = resourceGroup().location

@description('The pricing tier of workspace. Using standard to protect Azure for Student credits.')
@allowed([
  'standard'
  'premium'
])
param pricingTier string = 'standard'

// Define the Databricks Workspace Resource
resource databricksWorkspace 'Microsoft.Databricks/workspaces@2023-02-01' = {
  name: workspaceName
  location: location
  sku: {
    name: pricingTier
  }
  properties: {
    // Databricks requires a locked, managed resource group for its internal compute resources
    managedResourceGroupId: subscriptionResourceId('Microsoft.Resources/resourceGroups', '${workspaceName}-managed-rg')
  }
}

// Output the exact URL so our CI/CD and Makefile can consume it dynamically
output databricksWorkspaceUrl string = databricksWorkspace.properties.workspaceUrl