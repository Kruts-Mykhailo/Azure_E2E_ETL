from ctypes import ArgumentError
from azure.identity import DefaultAzureCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from dotenv import load_dotenv
import os
import json

load_dotenv()  # take environment variables from .env.

from azure.identity import DefaultAzureCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import (
    PipelineResource, 
)

# Authentication and Configuration
credential = DefaultAzureCredential()
subscription_id = os.getenv("SUBSCRIPTION_ID")
resource_group_name = "test-rg"
data_factory_name = "data-factory-source"

if not subscription_id:
    raise ArgumentError("SUBSCRIPTION_ID environment variable not set")

# Initialize the client
adf_client = DataFactoryManagementClient(credential, subscription_id)

# Load your JSON pipeline definition
with open("pipeline_with_dataflow.json", "r") as json_file:
    pipeline_definition = json.load(json_file)

# Deploy the pipeline
pipeline = PipelineResource.from_dict(pipeline_definition)
adf_client.pipelines.create_or_update(
    resource_group_name,
    data_factory_name,
    "TransformPipeline",
    pipeline
)

print("Pipeline deployed successfully.")
