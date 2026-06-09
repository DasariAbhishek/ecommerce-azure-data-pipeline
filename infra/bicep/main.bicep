// ---------------------------------------------------------------------------
// Azure infrastructure for the e-commerce medallion lakehouse.
// Provisions: ADLS Gen2 (bronze/silver/gold), Data Factory, Databricks,
// Key Vault. Deploy with:
//   az deployment group create -g <rg> -f main.bicep -p env=dev
// ---------------------------------------------------------------------------

@description('Short environment name, e.g. dev / prod')
param env string = 'dev'

@description('Azure region')
param location string = resourceGroup().location

var prefix = 'ecom${env}'
var storageName = toLower('${prefix}lake${uniqueString(resourceGroup().id)}')

// ---- ADLS Gen2 (hierarchical namespace) -----------------------------------
resource storage 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: storageName
  location: location
  sku: { name: 'Standard_LRS' }
  kind: 'StorageV2'
  properties: {
    isHnsEnabled: true            // enables Data Lake Gen2 semantics
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
  }
}

resource blobService 'Microsoft.Storage/storageAccounts/blobServices@2023-01-01' = {
  parent: storage
  name: 'default'
}

resource bronze 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  parent: blobService
  name: 'bronze'
}
resource silver 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  parent: blobService
  name: 'silver'
}
resource gold 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  parent: blobService
  name: 'gold'
}

// ---- Azure Data Factory (orchestration) -----------------------------------
resource adf 'Microsoft.DataFactory/factories@2018-06-01' = {
  name: '${prefix}-adf'
  location: location
  identity: { type: 'SystemAssigned' }
}

// ---- Azure Databricks (Spark compute) -------------------------------------
resource databricks 'Microsoft.Databricks/workspaces@2023-02-01' = {
  name: '${prefix}-dbw'
  location: location
  sku: { name: 'standard' }
  properties: {
    managedResourceGroupId: subscriptionResourceId(
      'Microsoft.Resources/resourceGroups',
      '${resourceGroup().name}-dbw-managed'
    )
  }
}

// ---- Key Vault (secrets) --------------------------------------------------
resource kv 'Microsoft.KeyVault/vaults@2023-07-01' = {
  name: '${prefix}-kv-${uniqueString(resourceGroup().id)}'
  location: location
  properties: {
    sku: { family: 'A', name: 'standard' }
    tenantId: subscription().tenantId
    enableRbacAuthorization: true
  }
}

output storageAccount string = storage.name
output dataFactory string = adf.name
output databricksWorkspace string = databricks.name
