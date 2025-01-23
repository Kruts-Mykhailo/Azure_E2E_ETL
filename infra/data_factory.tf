resource "azurerm_data_factory" "adf" {
  name                = "data-factory-source"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name

  identity {
    type = "SystemAssigned"
  }
}

resource "azurerm_data_factory_linked_service_data_lake_storage_gen2" "data-lake-ls" {
  name                  = "lake-linked-service"
  data_factory_id       = azurerm_data_factory.adf.id
  use_managed_identity  = true
  url                   = "https://${azurerm_storage_account.source-storage-acc.name}.dfs.core.windows.net"
  depends_on = [
    azurerm_storage_account.source-storage-acc
  ]
}

resource "azurerm_data_factory_linked_service_synapse" "adf-synapse-ls" {
  name            = "adf-synapse-ls"
  data_factory_id = azurerm_data_factory.adf.id
  connection_string =  <<EOT
    Integrated Security=False;
    Data Source=${azurerm_synapse_workspace.synapse-workspace.name}.sql.azuresynapse.net;
    Initial Catalog=${var.sql_synapse_db};
    User ID=${var.sql_admin_username};
    Password=${var.sql_admin_password};
  EOT

  depends_on = [
    azurerm_synapse_workspace.synapse-workspace,
    azurerm_synapse_sql_pool.synapase-sql-pool
  ]
}

resource "azurerm_role_assignment" "adf_blob_access" {
  principal_id         = azurerm_data_factory.adf.identity[0].principal_id
  role_definition_name = "Storage Blob Data Contributor"
  scope                = azurerm_storage_account.source-storage-acc.id
}
