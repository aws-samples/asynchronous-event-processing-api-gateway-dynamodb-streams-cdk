from aws_lambda_powertools import (
    Logger,
)
from boto3 import (
    client,
)
from boto3.dynamodb.types import (
    TypeDeserializer,
    TypeSerializer,
)
from os import (
    getenv,
)
from time import (
    sleep,
)

CONSUMER_ID = getenv("CONSUMER_ID")
OPTIMISTIC_LOCKING_RETRY_ATTEMPTS = int(
    getenv("OPTIMISTIC_LOCKING_RETRY_ATTEMPTS"))
TABLE_NAME = getenv("TABLE_NAME")
TIMEOUT = int(getenv("TIMEOUT"))
dynamodb = client("dynamodb")
logger = Logger(
    level=getenv("LOG_LEVEL", "DEBUG"),
    service="jobs_processing",
)


def dynamo_obj_to_python_obj(dynamo_obj: dict) -> dict:
    deserializer = TypeDeserializer()

    return {
        k: deserializer.deserialize(v)
        for k, v in dynamo_obj.items()
    }


def event_processing(seconds: int) -> str:
    message = f"I slept for {seconds} seconds"

    if seconds > TIMEOUT:
        raise ValueError(f"{seconds} major then {TIMEOUT}")

    sleep(seconds)

    return message


def python_obj_to_dynamo_obj(python_obj: dict) -> dict:
    serializer = TypeSerializer()
    return {
        k: serializer.serialize(v)
        for k, v in python_obj.items()
    }


def upsert(id: str, status: dict) -> None:
    for retry in range(OPTIMISTIC_LOCKING_RETRY_ATTEMPTS):
        try:
            logger.debug(f"Retry number {retry + 1} to update {id}")

            # Get existing item and its version
            item = dynamodb.get_item(
                Key={
                    "id": {
                        "S": id,
                    }
                },
                TableName=TABLE_NAME,
            )
            item_python = dynamo_obj_to_python_obj(item["Item"])
            item_current_version = item_python.get("version")
            item_status = item_python.get("job_status", {})

            logger.debug(f"Current version for {id} is {item_current_version}")
            logger.debug(f"Current status for {id} is {status}")

            # Set status for this consumer
            item_status[CONSUMER_ID] = status

            logger.debug(f"Updated status for {id} is {status}")

            # Try update DynamoDB item
            dynamodb.update_item(
                # Optimistic locking
                ConditionExpression="version = :cv",
                ExpressionAttributeValues={
                    ":cv": {
                        "N": str(item_current_version),
                    },
                    ":s": {
                        "M": python_obj_to_dynamo_obj(item_status),
                    },
                    ":v": {
                        "N": str(item_current_version + 1),
                    },
                },
                Key={
                    "id": {
                        "S": id,
                    },
                },
                ReturnValues="UPDATED_NEW",
                TableName=TABLE_NAME,
                UpdateExpression=f"SET job_status=:s, version=:v",
            )

            # Return when update is successful
            return
        except dynamodb.exceptions.ConditionalCheckFailedException:
            logger.warning("Failed to acquire lock, retrying")
        except Exception as exception:
            raise exception

    # Raise error when retry > max attempts
    raise RuntimeError(
        f"Max number of retries {OPTIMISTIC_LOCKING_RETRY_ATTEMPTS} exceeded")


def handler(event, context) -> None:
    """
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
    """
    logger.debug(context)
    logger.debug(event)

    id = event["Records"][0]["dynamodb"]["NewImage"]["id"]["S"]
    seconds = event["Records"][0]["dynamodb"]["NewImage"]["seconds"]["N"]

    logger.debug(f"Processing {id}")

    status_running = {
        "status": "Running",
    }

    upsert(id, status=status_running)

    status_done = {
        "results": event_processing(int(seconds)),
        "status": "Success",
    }

    upsert(id, status=status_done)
