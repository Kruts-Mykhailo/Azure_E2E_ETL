from azure.identity import DefaultAzureCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from dotenv import load_dotenv
import os

load_dotenv()  # take environment variables from .env.

# Configuration
subscription_id = os.getenv("SUBSCRIPTION_ID")
resource_group_name = "test-rg"
data_factory_name = "data-factory-source"
# Authenticate
credential = DefaultAzureCredential()
adf_client = DataFactoryManagementClient(credential, subscription_id)

# Create Mapping Data Flow
def create_mapping_data_flow():
    data_flow = {
        "properties": {
            "type": "MappingDataFlow",
            "typeProperties": {
                "sources": [
                    {
                        "name": "TripDataCSV",
                        "dataset": {"referenceName": "TripDataCSV"},
                        "source": {"type": "DelimitedTextSource"}
                    },
                    {
                        "name": "TripFaresCSV",
                        "dataset": {"referenceName": "TripFaresCSV"},
                        "source": {"type": "DelimitedTextSource"}
                    }
                ],
                "transformations": [
                    {
                        "name": "InnerJoinWithTripFares",
                        "type": "Join",
                        "inputs": ["TripDataCSV", "TripFaresCSV"],
                        "joinCondition": [
                            {"leftStream": "TripDataCSV", "leftField": "medallion", "rightStream": "TripFaresCSV", "rightField": "medallion"},
                            {"leftStream": "TripDataCSV", "leftField": "hack_license", "rightStream": "TripFaresCSV", "rightField": "hack_license"},
                            {"leftStream": "TripDataCSV", "leftField": "vendor_id", "rightStream": "TripFaresCSV", "rightField": "vendor_id"},
                            {"leftStream": "TripDataCSV", "leftField": "pickup_datetime", "rightStream": "TripFaresCSV", "rightField": "pickup_datetime"}
                        ],
                        "joinType": "Inner"
                    },
                    {
                        "name": "AggregateByPaymentType",
                        "type": "Aggregate",
                        "inputs": ["InnerJoinWithTripFares"],
                        "groupBy": [{"field": "payment_type"}],
                        "aggregates": [
                            {"field": "average_fare", "expression": "avg(toInteger(total_amount))"},
                            {"field": "total_trip_distance", "expression": "sum(toInteger(trip_distance))"}
                        ]
                    }
                ],
                "sinks": [
                    {
                        "name": "SynapseSink",
                        "dataset": {"referenceName": "SynapseTable"},
                        "type": "AzureSqlSink"
                    }
                ]
            }
        }
    }
    adf_client.data_flows.create_or_update(
        resource_group_name,
        data_factory_name,
        "JoinAndAggregateDataFlow",
        data_flow
    )

# Create Pipeline
def create_pipeline():
    pipeline = {
        "properties": {
            "activities": [
                {
                    "name": "RunDataFlow",
                    "type": "DataFlow",
                    "dataFlow": {"referenceName": "JoinAndAggregateDataFlow"},
                    "typeProperties": {
                        "staging": {"linkedService": {"referenceName": "AzureBlobStaging"}},
                        "sources": [
                            {"referenceName": "TripDataCSV"},
                            {"referenceName": "TripFaresCSV"}
                        ]
                    }
                }
            ]
        }
    }
    adf_client.pipelines.create_or_update(
        resource_group_name,
        data_factory_name,
        "JoinAndAggregatePipeline",
        pipeline
    )

# Execute

if __name__ == "__main__":
    create_mapping_data_flow()
    create_pipeline()
    print("ADF pipeline and data flow created successfully.")
