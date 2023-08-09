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
def dynamodb_stub(event_success: dict) -> Stubber:
    dynamodb_stub = Stubber(dynamodb)

    yield dynamodb_stub


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
    event_failure: dict,
) -> None:
    try:
        record = event_failure["Records"][0]["dynamodb"]["NewImage"]
        seconds = record["seconds"]["N"]
        timeout = int(getenv("TIMEOUT"))

        handler(event_failure, context)
    except ValueError as value_error:
        error_message = value_error.args[0]

        assert error_message == f"{seconds} major then {timeout}"  # nosec


def test_job_processing_success(
    context: LambdaContext,
    dynamodb_stub: Stubber,
    event_success: dict,
) -> None:
    with dynamodb_stub:
        handler(event_success, context)
