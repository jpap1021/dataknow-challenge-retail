output "resource_group_name" {
  value = azurerm_resource_group.main.name
}

output "storage_account_name" {
  value = azurerm_storage_account.datalake.name
}

output "data_factory_name" {
  value = azurerm_data_factory.main.name
}

output "key_vault_name" {
  value = azurerm_key_vault.main.name
}

output "databricks_workspace_url" {
  value = azurerm_databricks_workspace.main.workspace_url
}

output "log_analytics_workspace_name" {
  value = azurerm_log_analytics_workspace.main.name
}

output "adls_primary_dfs_endpoint" {
  value = azurerm_storage_account.datalake.primary_dfs_endpoint
}

output "bronze_container_name" {
  value = azurerm_storage_container.bronze.name
}

output "silver_container_name" {
  value = azurerm_storage_container.silver.name
}

output "gold_container_name" {
  value = azurerm_storage_container.gold.name
}