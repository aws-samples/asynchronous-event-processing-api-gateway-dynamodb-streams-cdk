from awslambdaric.lambda_context import (
    LambdaContext,
)
from botocore.stub import (
    Stubber,
)
from error_handling.main import (
    dynamodb,
    dynamodbstreams,
    handler,
)
from json import (
    dumps,
    loads,
)
from pytest import (
    fixture,
)
from tests.fixtures import (
    context,
)


@fixture
def dynamodb_stub(event: dict) -> Stubber:
    dynamodb_stub = Stubber(dynamodb)
    id = "1"

    dynamodb_stub.add_response(
        "get_item",
        expected_params={
            "Key": {
                "id": {
                    "S": id,
                },
            },
            "TableName": "jobs",
        },
        service_response={
            "Item": {
                "id": {
                    "S": id,
                },
                "job_status": {
                    "M": dict(),
                },
                "version": {
                    "N": "1",
                },
            },
        },
    )
    dynamodb_stub.add_response(
        "update_item",
        expected_params={
            "ConditionExpression": "version = :cv",
            "ExpressionAttributeValues": {
                ":cv": {
                    "N": "1",
                },
                ":s": {
                    "M": {
                        "consumer_1": {
                            "M": {
                                "seconds": {
                                    "S": "301",
                                },
                                "status": {
                                    "S": "Failure",
                                },
                            },
                        },
                    },
                },
                ":v": {
                    "N": "2",
                },
            },
            "Key": {
                "id": {
                    "S": id,
                },
            },
            "ReturnValues": "UPDATED_NEW",
            "TableName": "jobs",
            "UpdateExpression": f"SET job_status=:s, version=:v",
        },
        service_response=dict(),
    )

    yield dynamodb_stub


@fixture
def dynamodbstreams_stub(event: dict) -> Stubber:
    dynamodbstreams_stub = Stubber(dynamodbstreams)
    message = loads(event["Records"][0]["Sns"]["Message"])
    batch_info = message["DDBStreamBatchInfo"]
    shard_iterator = "000000000000000000000000"

    dynamodbstreams_stub.add_response(
        "get_shard_iterator",
        expected_params={
            "SequenceNumber": batch_info["startSequenceNumber"],
            "ShardId": batch_info["shardId"],
            "ShardIteratorType": "AT_SEQUENCE_NUMBER",
            "StreamArn": batch_info["streamArn"],
        },
        service_response={
            "ShardIterator": shard_iterator,
        },
    )
    dynamodbstreams_stub.add_response(
        "get_records",
        expected_params={
            "Limit": 1,
            "ShardIterator": shard_iterator,
        },
        service_response={
            "Records": [
                {
                    "dynamodb": {
                        "NewImage": {
                            "id": {
                                "S": "1",
                            },
                            "seconds": {
                                "N": "301",
                            },
                        },
                    },
                },
            ],
        },
    )

    yield dynamodbstreams_stub


@fixture
def event() -> dict:
    message = {
        "DDBStreamBatchInfo": {
            "startSequenceNumber": "000000000000000000000000",
            "shardId": "shardId-00000000000000000000",
            "streamArn": "arn:aws:dynamodb:us-east-1:012356789012:table/jobs/stream/0",
        },
    }
    event = {
        "Records": [
            {
                "Sns": {
                    "Message": dumps(message),
                },
            },
        ],
    }

    yield event


def test_error_handling(
    context: LambdaContext,
    dynamodb_stub: Stubber,
    dynamodbstreams_stub: Stubber,
    event: dict,
) -> None:
    with dynamodb_stub, dynamodbstreams_stub:
        handler(event, context)
