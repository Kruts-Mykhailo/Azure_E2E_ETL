resource "azurerm_storage_account" "synapse-storage" {
  name                     = "krutssinkstorage"
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = "true"
}

resource "azurerm_storage_data_lake_gen2_filesystem" "sink-filesystem" {
  name               = "sink"
  storage_account_id = azurerm_storage_account.synapse-storage.id
  depends_on = [azurerm_storage_account.synapse-storage]
}


resource "azurerm_synapse_workspace" "synapse-workspace" {
  name                                 = "process-workspace"
  resource_group_name                  = azurerm_resource_group.rg.name
  location                             = azurerm_resource_group.rg.location
  storage_data_lake_gen2_filesystem_id = azurerm_storage_data_lake_gen2_filesystem.sink-filesystem.id
  sql_administrator_login              = var.sql_admin_username
  sql_administrator_login_password     = var.sql_admin_password
  managed_virtual_network_enabled      = true

  identity {
    type = "SystemAssigned"
  }
}

resource "azurerm_synapse_sql_pool" "synapase-sql-pool" {
  name                 = var.sql_synapse_db
  synapse_workspace_id = azurerm_synapse_workspace.synapse-workspace.id
  sku_name             = "DW100c"
  create_mode          = "Default"
  storage_account_type = "GRS"
}

/******Firewall-rules-&-Role-assignments******/

resource "azurerm_synapse_firewall_rule" "allow-all-fr" {
  name                 = "allowAll"
  synapse_workspace_id = azurerm_synapse_workspace.synapse-workspace.id
  start_ip_address     = "0.0.0.0"
  end_ip_address       = "255.255.255.255"
}

resource "azurerm_synapse_integration_runtime_azure" "synapse-ir" {
  name                 = "SynapseIntegrationRuntime"
  synapse_workspace_id = azurerm_synapse_workspace.synapse-workspace.id
  location             = azurerm_resource_group.rg.location
}

resource "azurerm_synapse_role_assignment" "admin-role" {
  synapse_workspace_id = azurerm_synapse_workspace.synapse-workspace.id
  role_name            = "Synapse SQL Administrator"
  principal_id         = data.azurerm_client_config.current.object_id

  depends_on = [azurerm_synapse_firewall_rule.allow-all-fr]
}
