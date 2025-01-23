from ctypes import ArgumentError
from azure.identity import DefaultAzureCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import DatasetDataElement, DatasetResource, LinkedServiceReference, DelimitedTextDataset, AzureBlobStorageLocation
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
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

# Create Dataset for TripDataCSV
def create_trip_data_csv_dataset():
    ds_ls = LinkedServiceReference(type="LinkedServiceReference",reference_name="lake-linked-service")
    schema = [
            DatasetDataElement(name="id", type="Integer"),
            DatasetDataElement(name="medallion", type="String"),
            DatasetDataElement(name="hack_license", type="String"),
            DatasetDataElement(name="vendor_id", type="String"),
            DatasetDataElement(name="rate_code", type="Integer"),
            DatasetDataElement(name="store_and_fwd_flag", type="String"),
            DatasetDataElement(name="pickup_datetime", type="DateTime"),
            DatasetDataElement(name="dropoff_datetime", type="DateTime"),
            DatasetDataElement(name="passenger_count", type="Integer"),
            DatasetDataElement(name="trip_time_in_secs", type="Integer"),
            DatasetDataElement(name="trip_distance", type="Float"),
            DatasetDataElement(name="pickup_longitude", type="Float"),
            DatasetDataElement(name="pickup_latitude", type="Float"),
            DatasetDataElement(name="dropoff_longitude", type="Float"),
            DatasetDataElement(name="dropoff_latitude", type="Float"),
        ]
    
    dataset = DelimitedTextDataset(
        linked_service_name=ds_ls, 
        location=AzureBlobStorageLocation(
            container="source", # type: ignore
            file_name="TripDataNew.csv"  # Name of the file # type: ignore
        ),
        column_delimiter=",",  # Specify column delimiter # type: ignore
        first_row_as_header=True, # type: ignore
        schema=schema
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
            DatasetDataElement(name="id", type="Integer"),
            DatasetDataElement(name="medallion", type="String"),
            DatasetDataElement(name="hack_license", type="String"),
            DatasetDataElement(name="vendor_id", type="String"),
            DatasetDataElement(name="pickup_datetime", type="DateTime"),
            DatasetDataElement(name="payment_type", type="Integer"),
            DatasetDataElement(name="fare_amount", type="Float"),
            DatasetDataElement(name="surcharge", type="Float"),
            DatasetDataElement(name="mta_tax", type="Float"),
            DatasetDataElement(name="tip_amount", type="Float"),
            DatasetDataElement(name="tolls_amount", type="Float"),
            DatasetDataElement(name="total_amount", type="Float"),
        ]
    dataset = DelimitedTextDataset(
        linked_service_name=ds_ls, 
        location=AzureBlobStorageLocation(
            container="source", # type: ignore
            file_name="TripFaresNew.csv"  # Name of the file # type: ignore
        ),
        column_delimiter=",",  # Specify column delimiter # type: ignore
        first_row_as_header=True, # type: ignore 
        schema=schema
    )

    

    dataset_blob = DatasetResource(properties=dataset)
    ds = adf_client.datasets.create_or_update(
        resource_group_name=resource_group_name,
        factory_name=data_factory_name,
        dataset_name='TripFares',
        dataset=dataset_blob
    )


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
    upload_file_to_data_lake(local_file_path_trip_data, "TripDataNew.csv")
    upload_file_to_data_lake(local_file_path_trip_fares, "TripFaresNew.csv")

    create_trip_data_csv_dataset()
    create_trip_fares_csv_dataset()
    print("Datasets created successfully.")
