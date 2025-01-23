resource "azurerm_storage_account" "source-storage-acc" {
  name                     = var.storage_account_name
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = "true"
  default_to_oauth_authentication = true
}

resource "azurerm_storage_data_lake_gen2_filesystem" "source-filesystem" {
  name               = "source"
  storage_account_id = azurerm_storage_account.source-storage-acc.id
  depends_on = [azurerm_storage_account.source-storage-acc]
}

resource "azurerm_storage_account_network_rules" "network_rules" {
  storage_account_id = azurerm_storage_account.source-storage-acc.id

  # Allow traffic from ADF
  bypass                     = ["AzureServices"]
  default_action             = "Deny"
  ip_rules                  = ["${var.client_ip}/30"]
  virtual_network_subnet_ids = []  # Use this if ADF is on a VNET
}

resource "azurerm_role_assignment" "assign_my_identity" {
  scope                = azurerm_storage_account.source-storage-acc.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = data.azurerm_client_config.current.object_id # Your Azure identity
}