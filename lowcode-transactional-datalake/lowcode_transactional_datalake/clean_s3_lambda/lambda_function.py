import json
import boto3
import os

BUCKET = os.environ['BUCKET']
PREFIX = os.environ['PREFIX']


def lambda_handler(event, context):
    print(json.dumps(event))
    client = boto3.client('s3')
    objects = list_all(client)
    if len(objects) > 0:
        response = client.delete_objects(
            Bucket=BUCKET,
            Delete={
                'Objects': objects,
                'Quiet': False
            },
        )
        print(f"Deleted: {response['Deleted']}")
        print(f"Errors: {response.get('Errors', None)}")
    else:
        print('No objects need to be deleted.')

    return 'succeeded'


def list_all(client):
    response = client.list_objects_v2(
        Bucket=BUCKET,
        MaxKeys=100,
        Prefix=PREFIX,
        # ContinuationToken='string',
    )
    objects = [
        {'Key': content['Key']}
        for content in response['Contents']
    ]
    next_token = response.get('ContinuationToken', None)
    while next_token:
        response = client.list_objects_v2(
            Bucket=BUCKET,
            MaxKeys=100,
            Prefix=PREFIX,
            ContinuationToken=next_token,
        )
        objects = objects + [
            {'Key': content['Key']}
            for content in response['Contents']
        ]
        next_token = response.get('ContinuationToken', None)
    return objects
