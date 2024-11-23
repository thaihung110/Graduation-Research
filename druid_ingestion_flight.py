import json
import time

import requests


def submit_to_druid(ingestion_spec):
    # Use the service name 'coordinator' as defined in docker-compose for the Druid Overlord URL
    druid_overlord_url = "http://coordinator:8081/druid/indexer/v1/task"
    headers = {"Content-Type": "application/json"}
    response = requests.post(
        druid_overlord_url, headers=headers, data=json.dumps(ingestion_spec)
    )
    if response.status_code == 200:
        print("Ingestion task submitted successfully.")
    else:
        print(f"Failed to submit ingestion task: {response.text}")


def submit_dim_date_ingestion_spec():
    ingestion_spec = {
        "type": "index_parallel",
        "spec": {
            "dataSchema": {
                "dataSource": "dim_date",
                "timestampSpec": {"column": "dateKey", "format": "yyyyMMdd"},
                "dimensionsSpec": {
                    "dimensions": [
                        "dateKey",
                        "date",
                        "year",
                        "month",
                        "dayOfMonth",
                        "dayOfWeek",
                        "quarter",  # Add other dimension columns here
                    ]
                },
            },
            "ioConfig": {
                "type": "index_parallel",
                "inputSource": {
                    "type": "local",
                    "baseDir": "/opt/airflow/warehouse",
                    "filter": "dim_date.csv",
                },
                "inputFormat": {"type": "csv", "findColumnsFromHeader": True},
            },
            "tuningConfig": {
                "type": "index_parallel",
                "maxNumConcurrentSubTasks": 1,
            },
        },
    }
    submit_to_druid(ingestion_spec)
    time.sleep(20)


def submit_dim_airport_ingestion_spec():
    ingestion_spec = {
        "type": "index_parallel",
        "spec": {
            "dataSchema": {
                "dataSource": "dim_airport",
                "timestampSpec": {
                    "column": "",  # No column to specify
                    "format": "auto",
                    "missingValue": "1970-01-01T00:00:00Z",
                },
                "dimensionsSpec": {"dimensions": ["airportKey", "airportCode"]},
            },
            "ioConfig": {
                "type": "index_parallel",
                "inputSource": {
                    "type": "local",
                    "baseDir": "/opt/airflow/warehouse",
                    "filter": "dim_airport.csv",
                },
                "inputFormat": {"type": "csv", "findColumnsFromHeader": True},
            },
            "tuningConfig": {
                "type": "index_parallel",
                "maxNumConcurrentSubTasks": 1,
            },
        },
    }
    submit_to_druid(ingestion_spec)
    time.sleep(20)


def submit_dim_airline_ingestion_spec():
    ingestion_spec = {
        "type": "index_parallel",
        "spec": {
            "dataSchema": {
                "dataSource": "dim_airline",
                "timestampSpec": {
                    "column": "",  # No column to specify
                    "format": "auto",
                    "missingValue": "1970-01-01T00:00:00Z",
                },
                "dimensionsSpec": {
                    "dimensions": [
                        "airlineKey",
                        "airlineName",  # Add other dimension columns here
                    ]
                },
            },
            "ioConfig": {
                "type": "index_parallel",
                "inputSource": {
                    "type": "local",
                    "baseDir": "/opt/airflow/warehouse",
                    "filter": "dim_airline.csv",
                },
                "inputFormat": {"type": "csv", "findColumnsFromHeader": True},
            },
            "tuningConfig": {
                "type": "index_parallel",
                "maxNumConcurrentSubTasks": 1,
            },
        },
    }
    submit_to_druid(ingestion_spec)
    time.sleep(20)


def submit_fact_flight_ingestion_spec():
    ingestion_spec = {
        "type": "index_parallel",
        "spec": {
            "dataSchema": {
                "dataSource": "fact_flight",
                "timestampSpec": {"column": "dateKey", "format": "yyyyMMdd"},
                "dimensionsSpec": {
                    "dimensions": [
                        "flightKey",
                        "isBasicEconomy",
                        "isRefundable",
                        "isNonStop",
                        "totalFare",
                        "travelDuration",
                        "departureTime",
                        "arrivalTime",
                        "dateKey",
                        "airlineKey",
                        "startingAirportKey",
                        "destinationAirportKey",
                    ]
                },
            },
            "ioConfig": {
                "type": "index_parallel",
                "inputSource": {
                    "type": "local",
                    "baseDir": "/opt/airflow/warehouse",
                    "filter": "fact_flight.csv",
                },
                "inputFormat": {"type": "csv", "findColumnsFromHeader": True},
            },
            "tuningConfig": {
                "type": "index_parallel",
                "maxNumConcurrentSubTasks": 1,
            },
        },
    }
    submit_to_druid(ingestion_spec)
    time.sleep(20)
