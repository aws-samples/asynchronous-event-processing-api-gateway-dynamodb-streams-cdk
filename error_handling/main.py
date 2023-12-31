from awslambdaric.lambda_context import (
    LambdaContext,
)
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
from json import (
    loads,
)
from os import (
    getenv,
)

CONSUMER_ID = getenv("CONSUMER_ID")
OPTIMISTIC_LOCKING_RETRY_ATTEMPTS = int(
    getenv("OPTIMISTIC_LOCKING_RETRY_ATTEMPTS"))
TABLE_NAME = getenv("TABLE_NAME")
dynamodb = client("dynamodb")
dynamodbstreams = client("dynamodbstreams")
logger = Logger(
    level=getenv("LOG_LEVEL", "DEBUG"),
    service="error_handling",
)


def dynamo_obj_to_python_obj(dynamo_obj: dict) -> dict:
    deserializer = TypeDeserializer()

    return {
        k: deserializer.deserialize(v)
        for k, v in dynamo_obj.items()
    }


def get_record(message: dict) -> str:
    batch_info = message["DDBStreamBatchInfo"]
    shard_iterator = dynamodbstreams.get_shard_iterator(
        SequenceNumber=batch_info["startSequenceNumber"],
        ShardId=batch_info["shardId"],
        ShardIteratorType="AT_SEQUENCE_NUMBER",
        StreamArn=batch_info["streamArn"],
    )
    record = dynamodbstreams.get_records(
        Limit=1,  # Only one event at a time is processed
        ShardIterator=shard_iterator["ShardIterator"],
    )

    return record["Records"][0]["dynamodb"]["NewImage"]


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


def handler(event: dict, context: LambdaContext) -> None:
    logger.debug(context)
    logger.debug(event)

    message = loads(event["Records"][0]["Sns"]["Message"])
    record = get_record(message)

    logger.debug(f"Processing {record['id']['S']}")

    status_failure = {
        "seconds": record["seconds"]["N"],
        "status": "Failure",
    }

    upsert(record["id"]["S"], status_failure)
