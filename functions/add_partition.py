import boto3
import os

database = os.getenv('DATABASE')
table = os.getenv('TABLE')

def add_partition(database, table, timestamp, location):
    client = boto3.client('athena')
    response = client.start_query_execution(
        QueryString='ALTER TABLE {} ADD PARTITION (load_timestamp = {}) LOCATION {}'.format(table, timestamp, location),
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': 'string',
        }
    )

    return response

def lambda_handler(event, context):
    key = event['Records'][0]['s3']['object']['key'].replace('%3D', '=')
    bucket = event['Records'][0]['s3']['object']['bucket']
    location = 's3//{}/{}'.format(bucket,key) # Replaces the encoded = to a regular usable = 
    timestamp = key.split('=')[-1] # Retrieves just the key timestamp
    add_partition(database,table, timestamp, location)