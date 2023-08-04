from aws_lambda_powertools import (
    Logger,
)
from boto3 import (
    client,
)
from boto3.dynamodb.types import (
    TypeSerializer,
    TypeDeserializer,
)
from os import (
    getenv,
)
from time import (
    sleep,
)
import traceback

CONSUMER_ID = getenv("CONSUMER_ID")
OPTIMISTIC_LOCKING_RETRY_ATTEMPTS = int(getenv("OPTIMISTIC_LOCKING_RETRY_ATTEMPTS"))
TABLE_NAME = getenv("TABLE_NAME")
TIMEOUT = int(getenv("TIMEOUT"))

dynamodb = client("dynamodb")
logger = Logger(
    level=getenv("LOG_LEVEL", "INFO"),
    service="jobs_processing",
)


def event_processing(seconds: int) -> str:
    message = f"I slept for {seconds} seconds"
    if seconds > TIMEOUT:
        raise ValueError(f"{seconds} major then {TIMEOUT}")
    sleep(seconds)
    return message


def dynamo_obj_to_python_obj(dynamo_obj: dict) -> dict:
    deserializer = TypeDeserializer()
    return {
        k: deserializer.deserialize(v)
        for k, v in dynamo_obj.items()
    }


def python_obj_to_dynamo_obj(python_obj: dict) -> dict:
    serializer = TypeSerializer()
    return {
        k: serializer.serialize(v)
        for k, v in python_obj.items()
    }


def upsert(id: str, status: dict):
    for retry in range(OPTIMISTIC_LOCKING_RETRY_ATTEMPTS):
        try:
            logger.info(f"Try number {retry} to update {id}")

            # Get existing item and its version
            item = dynamodb.get_item(
                TableName=TABLE_NAME,
                Key={
                    'id': {
                        'S': id,
                    }}
            )
            item_python = dynamo_obj_to_python_obj(item["Item"])
            item_current_version = item_python.get("version")
            item_status = item_python.get("job_status", {})
            logger.info(f'Current version for {id} is {item_current_version}')
            logger.info(f'Current status for {id} is {status}')

            # Set running state for this consumer
            item_status[CONSUMER_ID] = status
            logger.info(f'Updated status for {id} is {status}')

            # Try update
            dynamodb.update_item(
                TableName=TABLE_NAME,
                Key={
                    "id": {
                        "S": id,
                    }},
                # Optimistic locking condition - the upstream version should be the same
                # when updating this item, otherwise throw an exception
                ConditionExpression=f"version = :cv",
                UpdateExpression=f"SET job_status=:s, version=:v",
                ExpressionAttributeValues={
                    ':s': {"M": python_obj_to_dynamo_obj(item_status)},
                    ':v': {"N": str(item_current_version + 1)},
                    ':cv': {"N": str(item_current_version)}
                },
                ReturnValues="UPDATED_NEW"
            )
        except Exception as e:
            logger.error(e)
            traceback.print_exc()
        break


def handler(event, context) -> None:
    '''
    The input event is in the following format:

    "Records": [
        {
            "eventID": "bc391aaxxxxxxxxxxxxb9da61ad3bc",
            "eventName": "INSERT",
            "eventVersion": "1.1",
            "eventSource": "aws:dynamodb",
            "awsRegion": "us-east-1",
            "dynamodb": {
                "ApproximateCreationDateTime": 1689605602,
                "Keys": {
                    "id": {
                        "S": "4881664f-d59e-44c3-ba76-01fbec586f37"
                    }
                },
                "NewImage": {
                    "job_status": {
                        "M": {}
                    },
                    "seconds": {
                        "N": "60"
                    },
                    "id": {
                        "S": "4881664f-d59e-44c3-ba76-01fbec586f37"
                    },
                    "version": {
                        "N": "0"
                    }
                },
                "SequenceNumber": "946475000000000000011227028182",
                "SizeBytes": 106,
                "StreamViewType": "NEW_IMAGE"
            },
            "eventSourceARN": "arn:aws:dynamodb:us-east-1:xxxxxxxxx:table/AsynchronousProcessingAPIGatewayDynamoDBStream-EventProcessingJobsTablexxxxxxxxx/stream/2023-06-27T15:24:31.102"
        }
    ]
    '''
    logger.info(event)
    logger.info(context)

    id = event["Records"][0]["dynamodb"]["NewImage"]["id"]["S"]
    seconds = event["Records"][0]["dynamodb"]["NewImage"]["seconds"]["N"]
    logger.info(f"Processing {id}...")

    status_running = {
        "state": "running",
    }
    upsert(id, status=status_running)

    results = event_processing(int(seconds))

    status_done = {
        "state": "done",
        "results": results,
    }
    upsert(id, status=status_done)
