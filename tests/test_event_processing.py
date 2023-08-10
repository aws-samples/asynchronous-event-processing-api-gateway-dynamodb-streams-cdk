from awslambdaric.lambda_context import (
    LambdaContext,
)
from botocore.stub import (
    Stubber,
)
from event_processing.main import (
    dynamodb,
    handler,
)
from os import (
    getenv,
)
from pytest import (
    fixture,
)
from tests.fixtures import (
    context,
)


@fixture
def dynamodb_stub_failure(event_failure: dict) -> Stubber:
    dynamodb_stub_failure = Stubber(dynamodb)
    id = event_failure["Records"][0]["dynamodb"]["NewImage"]["id"]["S"]

    dynamodb_stub_failure.add_response(
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
    dynamodb_stub_failure.add_response(
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
                                "status": {
                                    "S": "Running",
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

    yield dynamodb_stub_failure


@fixture
def dynamodb_stub_success(event_success: dict) -> Stubber:
    dynamodb_stub_success = Stubber(dynamodb)
    id = event_success["Records"][0]["dynamodb"]["NewImage"]["id"]["S"]
    seconds = event_success["Records"][0]["dynamodb"]["NewImage"]["seconds"]["N"]

    dynamodb_stub_success.add_response(
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
    dynamodb_stub_success.add_response(
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
                                "status": {
                                    "S": "Running",
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
    dynamodb_stub_success.add_response(
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
                    "M": {
                        "consumer_1": {
                            "M": {
                                "status": {
                                    "S": "Running",
                                },
                            },
                        },
                    },
                },
                "version": {
                    "N": "2",
                },
            },
        },
    )
    dynamodb_stub_success.add_response(
        "update_item",
        expected_params={
            "ConditionExpression": "version = :cv",
            "ExpressionAttributeValues": {
                ":cv": {
                    "N": "2",
                },
                ":s": {
                    "M": {
                        "consumer_1": {
                            "M": {
                                "results": {
                                    "S": f"I slept for {seconds} seconds",
                                },
                                "status": {
                                    "S": "Success",
                                },
                            },
                        },
                    },
                },
                ":v": {
                    "N": "3",
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

    yield dynamodb_stub_success


@fixture
def event_failure() -> dict:
    event_failure = {
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
    }

    yield event_failure


@fixture
def event_success() -> dict:
    event_success = {
        "Records": [
            {
                "dynamodb": {
                    "NewImage": {
                        "id": {
                            "S": "2",
                        },
                        "seconds": {
                            "N": "1",
                        },
                    },
                },
            },
        ],
    }

    yield event_success


def test_job_processing_failure(
    context: LambdaContext,
    dynamodb_stub_failure: Stubber,
    event_failure: dict,
) -> None:
    try:
        record = event_failure["Records"][0]["dynamodb"]["NewImage"]
        seconds = record["seconds"]["N"]
        timeout = int(getenv("TIMEOUT"))

        with dynamodb_stub_failure:
            handler(event_failure, context)
    except ValueError as value_error:
        error_message = value_error.args[0]

        assert error_message == f"{seconds} major then {timeout}"  # nosec


def test_job_processing_success(
    context: LambdaContext,
    dynamodb_stub_success: Stubber,
    event_success: dict,
) -> None:
    with dynamodb_stub_success:
        handler(event_success, context)
