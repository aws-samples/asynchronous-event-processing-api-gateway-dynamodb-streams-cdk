from infrastructure.main import (
    InfrastructureStack,
)
from aws_cdk import (
    App,
)

app = App()

InfrastructureStack(
    app,
    "AsynchronousProcessingAPIGatewayDynamoDBStream",
    description="Asynchronous Processing with API Gateway and DynamoDB Streams",
)

app.synth()
