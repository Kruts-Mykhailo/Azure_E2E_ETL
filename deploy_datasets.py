from ctypes import ArgumentError
from azure.identity import DefaultAzureCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import AzureSqlDWTableDataset, CopyActivity, DatasetDataElement, DatasetReference, DatasetReferenceType, DatasetResource, LinkedServiceReference, DelimitedTextDataset, AzureBlobStorageLocation, PipelineResource, SqlSink, SqlSource
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from pyodbc import connect
import os

load_dotenv()  # take environment variables from .env.

# Configuration``
subscription_id = os.getenv("SUBSCRIPTION_ID")
resource_group_name = "test-rg"
data_factory_name = "data-factory-source"
storage_account_name = "mykhailostoragenew"
container_name = "source"
local_file_path_trip_data = "./data/TripDataNew.csv"
local_file_path_trip_fares = "./data/TripFaresNew.csv"

if not subscription_id:
    raise ArgumentError("SUBSCRIPTION_ID environment variable not set")


# Authenticate
credential = DefaultAzureCredential()
adf_client = DataFactoryManagementClient(credential, subscription_id)

def connect_to_synapse():
    server_name = os.getenv("SERVER_NAME")
    database_name = os.getenv("DATABASE")
    username = os.getenv("USERNAME")
    password = os.getenv("PASSWORD")
    
    conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server_name};DATABASE={database_name};UID={username};PWD={password}"
    connection = connect(conn_str)
    return connection

# Execute the Query
def execute_query(connection):
    create_table_query = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='TransformedTripData' AND xtype='U')
    BEGIN
        CREATE TABLE dbo.TransformedTripData (
            payment_type INT,
            average_fare FLOAT,
            total_trip_distance FLOAT
        );
    END;
    """
    
    with connection.cursor() as cursor:
        cursor.execute(create_table_query)
        print("Table created successfully (if not already present).")


# Create Dataset for TripDataCSV
def create_trip_data_csv_dataset():
    ds_ls = LinkedServiceReference(type="LinkedServiceReference",reference_name="lake-linked-service")
    schema = [
            DatasetDataElement(name="id", type="Integer"), # type: ignore
            DatasetDataElement(name="medallion", type="String"), # type: ignore
            DatasetDataElement(name="hack_license", type="String"), # type: ignore
            DatasetDataElement(name="vendor_id", type="String"), # type: ignore
            DatasetDataElement(name="rate_code", type="Integer"), # type: ignore
            DatasetDataElement(name="store_and_fwd_flag", type="String"), # type: ignore
            DatasetDataElement(name="pickup_datetime", type="DateTime"), # type: ignore
            DatasetDataElement(name="dropoff_datetime", type="DateTime"), # type: ignore
            DatasetDataElement(name="passenger_count", type="Integer"), # type: ignore
            DatasetDataElement(name="trip_time_in_secs", type="Integer"), # type: ignore
            DatasetDataElement(name="trip_distance", type="Float"), # type: ignore
            DatasetDataElement(name="pickup_longitude", type="Float"), # type: ignore
            DatasetDataElement(name="pickup_latitude", type="Float"), # type: ignore
            DatasetDataElement(name="dropoff_longitude", type="Float"), # type: ignore
            DatasetDataElement(name="dropoff_latitude", type="Float"), # type: ignore
        ]
    
    dataset = DelimitedTextDataset(
        linked_service_name=ds_ls, 
        location=AzureBlobStorageLocation(
            container="source", # type: ignore
            file_name="TripDataNew.csv"  # Name of the file # type: ignore
        ),
        column_delimiter=",",  # Specify column delimiter # type: ignore
        first_row_as_header=True, # type: ignore
        schema=schema # type: ignore
        )  # Specify if the first row contains headers)
    
    
    dataset_blob = DatasetResource(properties=dataset)
    ds = adf_client.datasets.create_or_update(
        resource_group_name=resource_group_name,
        factory_name=data_factory_name,
        dataset_name='TripData',
        dataset=dataset_blob
    )


# Create Dataset for TripFaresCSV
def create_trip_fares_csv_dataset():
    ds_ls = LinkedServiceReference(type="LinkedServiceReference",reference_name="lake-linked-service")
    schema = [
            DatasetDataElement(name="id", type="Integer"),  # type: ignore
            DatasetDataElement(name="medallion", type="String"), # type: ignore
            DatasetDataElement(name="hack_license", type="String"), # type: ignore
            DatasetDataElement(name="vendor_id", type="String"), # type: ignore
            DatasetDataElement(name="pickup_datetime", type="DateTime"), # type: ignore
            DatasetDataElement(name="payment_type", type="Integer"), # type: ignore
            DatasetDataElement(name="fare_amount", type="Float"), # type: ignore
            DatasetDataElement(name="surcharge", type="Float"), # type: ignore
            DatasetDataElement(name="mta_tax", type="Float"), # type: ignore
            DatasetDataElement(name="tip_amount", type="Float"), # type: ignore
            DatasetDataElement(name="tolls_amount", type="Float"), # type: ignore
            DatasetDataElement(name="total_amount", type="Float"), # type: ignore
        ]
    dataset = DelimitedTextDataset(
        linked_service_name=ds_ls, 
        location=AzureBlobStorageLocation(
            container="source", # type: ignore
            file_name="TripFaresNew.csv"  # Name of the file # type: ignore
        ),
        column_delimiter=",",  # Specify column delimiter # type: ignore
        first_row_as_header=True, # type: ignore 
        schema=schema # type: ignore
    )

    

    dataset_blob = DatasetResource(properties=dataset)
    ds = adf_client.datasets.create_or_update(
        resource_group_name=resource_group_name,
        factory_name=data_factory_name,
        dataset_name='TripFares',
        dataset=dataset_blob
    )

def create_synapse_sink_dataset():
    ds_ls = LinkedServiceReference(type="LinkedServiceReference",reference_name="adf-synapse-ls")
    dataset = AzureSqlDWTableDataset(
        linked_service_name=ds_ls, 
        table_name="dbo.TransformedTripData" # type: ignore
    )

    dataset_blob = DatasetResource(properties=dataset)
    ds = adf_client.datasets.create_or_update(
        resource_group_name=resource_group_name,
        factory_name=data_factory_name,
        dataset_name='TransformedTripData',
        dataset=dataset_blob
    )
    
    print("Synapse sink dataset created successfully.")


def upload_file_to_data_lake(file_path, file_name):
    try:
        blob_service_client = BlobServiceClient(
            f"https://{storage_account_name}.blob.core.windows.net", credential=credential
        )
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)

        # Upload or update the file
        with open(file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        print(f"File '{file_name}' uploaded to container '{container_name}' successfully.")
    except Exception as e:
        print(f"Error uploading file '{file_name}': {e}")

# Execute
if __name__ == "__main__":
    # upload_file_to_data_lake(local_file_path_trip_data, "TripDataNew.csv")
    # upload_file_to_data_lake(local_file_path_trip_fares, "TripFaresNew.csv")
    # conn = connect_to_synapse()
    # execute_query(conn)
    # conn.close()
    # create_trip_data_csv_dataset()
    # create_trip_fares_csv_dataset()
    create_synapse_sink_dataset()


    print("Datasets created successfully.")
