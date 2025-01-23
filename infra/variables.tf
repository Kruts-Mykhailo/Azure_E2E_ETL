variable "resource_group_name" {
  description = "Name of the Azure Resource Group"
  type        = string
}

variable "location" {
  description = "Azure region for the resources"
  type        = string
  default     = "East US"
}

variable "sql_admin_username" {
  description = "Administrator username for the Azure SQL Server"
  type        = string
}

variable "sql_admin_password" {
  description = "Administrator password for the Azure SQL Server"
  type        = string
  sensitive   = true
}

variable "sql_synapse_db" {
    description = "SQL pool database name for Azure Synapse Analytics"
    default = "user_sink_db"
}

variable "storage_account_name" {
  description = "Name of the Azure Storage Account"
  type        = string
}

variable "data_factory_name" {
  description = "Name of the Azure Data Factory"
  type        = string
}
variable "client_ip" {
  default = "0.0.0.0"
}