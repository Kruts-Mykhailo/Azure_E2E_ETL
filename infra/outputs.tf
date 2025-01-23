# output "sql_server_fqdn" {
#   value = azurerm_postgresql_flexible_server.postgres-server.fqdn
# }

output "storage_account_connection_string" {
  value       = azurerm_storage_account.source-storage-acc.primary_connection_string
  sensitive   = true
}

output "data_factory_identity" {
  value = azurerm_data_factory.adf.identity[0].principal_id
}
