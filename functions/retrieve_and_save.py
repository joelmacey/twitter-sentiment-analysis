import tweepy
import csv
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
import uuid
import json
import base64
import os
from pyarrow import csv as pqcsv
import pyarrow.parquet as pq

def get_secret(secret_name):

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager'
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
        return get_secret_value_response
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            return secret
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            return decoded_binary_secret

def get_auth(secret_name):
    #https://tweepy.readthedocs.io/en/latest/auth_tutorial.html#auth-tutorial
    
    secrets = json.loads(get_secret(secret_name)['SecretString'])
    
    consumer_key = secrets['consumer_key']
    consumer_secret = secrets['consumer_secret']
    access_token = secrets['access_token']
    access_token_secret = secrets['access_token_secret']

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth,wait_on_rate_limit=True)
    return api    

def create_since_timestamp():
    timestamp = datetime.utcnow().strftime('%Y-%m-%d')
    return timestamp

def get_data(api, query, since_date):
    data = []
    count = 0
    #https://gist.github.com/vickyqian/f70e9ab3910c7c290d9d715491cde44c
    for tweet in tweepy.Cursor(api.search,q=query,count=100,
    lang="en",
    since=since_date).items():
        line = []
        id = str(uuid.uuid4())
        line.append(id)
        line.append(tweet.created_at)
        line.append(count)
        line.append(tweet.text.encode('utf-8'))
        count +=1
        data.append(line)
    return data

def create_timestamp():
    timestamp = datetime.utcnow().strftime('%Y%m%d%H%M%S')
    return timestamp

def create_path_timestamp():
    timestamp = datetime.utcnow().strftime('year=%Y/month=%m/day=%d')
    return timestamp

def to_dynamodb(data, table_name):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    for i in range(len(data)):
        table.put_item(
            Item={
                'id': data[i][0],
                'creation_date': str(data[i][1]),
                'line': data[i][2],
                'text': data[i][3].decode()        
            }
        )

def to_csv_file(data, wd):
    timestamp = create_timestamp()
    filename = '{}.csv'.format(timestamp)
    with open ('{}/{}'.format(wd, filename), 'w', newline='') as csvfile:
        csv_w = csv.writer(csvfile, delimiter=',', quotechar= '"')
        for row in range(len(data)):
            row_att = []
            for record in range(len(data[row])):
                row_att.append(data[row][record])
            csv_w.writerow(row_att) # Write the row
    csvfile.close()
    return filename

def to_parquet_file(filename, wd):
    csv_file = '{}/{}'.format(wd,filename)
    table = pqcsv.read_csv(csv_file)
    pq_filename = filename.split('.')[0]
    pq.write_table(table, '{}.parquet'.format(pq_filename))
    return '{}.parquet'.format(pq_filename)


def send_to_s3(bucket_path, wd, filename, bucket):
    client = boto3.client('s3')
    path = create_path_timestamp()
    response = client.put_object(
        Body='{}/{}'.format(wd,filename),
        Bucket=bucket,
        Key='{}/{}/{}'.format(bucket_path,path, filename)
    )
    return response

def remove_file(wd,filename):
    if os.path.exists('{}/{}'.format(wd,filename)):
        os.remove('{}/{}'.format(wd,filename))
        return 'Deleted Successfully'
    else:
        return 'Failed to delete file'

def lambda_handler(event, context):

    wd = './' # tmp for lambda
    query = os.getenv('QUERY')
    bucket = os.getenv('BUCKET')
    bucket_path = os.getenv('BUCKET_PATH')
    table_name = os.getenv('TABLE_NAME')
    secret_name = os.getenv('SECRET_NAME')
    api = get_auth(secret_name)
    since_date=create_since_timestamp()
    data = get_data(api, query, since_date)
    filename = to_csv_file(data, wd)
    pq_filename = to_parquet_file(filename, wd)
    to_dynamodb(data, table_name)
    response = send_to_s3(bucket_path, wd, pq_filename, bucket)
    remove_file(wd,filename) # original csv
    remove_file(wd,pq_filename) # new parquet
    
    return response