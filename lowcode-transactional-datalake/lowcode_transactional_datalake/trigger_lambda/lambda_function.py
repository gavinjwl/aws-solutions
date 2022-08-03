import uuid
import json
import os

import boto3

STATE_MACHINE_ARN = os.environ['STATE_MACHINE_ARN']


def lambda_handler(event, context):
    print(json.dumps(event))
    client = boto3.client('stepfunctions')
    response = client.start_execution(
        stateMachineArn=STATE_MACHINE_ARN,
        name=str(uuid.uuid4()),
    )
    print(response)
