# ---------------------------------------------------------------------------
# Terraform equivalent of the Bicep template (for teams standardized on TF).
#   terraform init && terraform apply -var="env=dev"
# ---------------------------------------------------------------------------

terraform {
  required_version = ">= 1.5"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.110"
    }
  }
}

provider "azurerm" {
  features {}
}

variable "env" {
  type    = string
  default = "dev"
}

variable "location" {
  type    = string
  default = "eastus"
}

locals {
  prefix = "ecom${var.env}"
}

resource "azurerm_resource_group" "rg" {
  name     = "rg-${local.prefix}-dataeng"
  location = var.location
}

# ADLS Gen2
resource "azurerm_storage_account" "lake" {
  name                     = "${local.prefix}lake${substr(md5(azurerm_resource_group.rg.id), 0, 8)}"
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true # Data Lake Gen2
  min_tls_version          = "TLS1_2"
}

resource "azurerm_storage_container" "layers" {
  for_each              = toset(["bronze", "silver", "gold"])
  name                  = each.key
  storage_account_name  = azurerm_storage_account.lake.name
  container_access_type = "private"
}

# Data Factory
resource "azurerm_data_factory" "adf" {
  name                = "${local.prefix}-adf"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  identity { type = "SystemAssigned" }
}

# Databricks
resource "azurerm_databricks_workspace" "dbw" {
  name                = "${local.prefix}-dbw"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku                 = "standard"
}

output "storage_account" {
  value = azurerm_storage_account.lake.name
}
output "data_factory" {
  value = azurerm_data_factory.adf.name
}
output "databricks_workspace" {
  value = azurerm_databricks_workspace.dbw.name
}
