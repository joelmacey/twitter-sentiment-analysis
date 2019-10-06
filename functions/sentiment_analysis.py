import boto3

def get_sentiment():
    client = boto3.client('comprehend')
    response = client.start_sentiment_detection_job(
        InputDataConfig={
            'S3Uri': 'string',
            'InputFormat': 'ONE_DOC_PER_FILE'|'ONE_DOC_PER_LINE'
        },
        OutputDataConfig={
            'S3Uri': 'string',
            'KmsKeyId': 'string'
        },
        DataAccessRoleArn='string',
        JobName='string',
        LanguageCode='en'|'es'|'fr'|'de'|'it'|'pt',
        ClientRequestToken='string',
        VolumeKmsKeyId='string',
        VpcConfig={
            'SecurityGroupIds': [
                'string',
            ],
            'Subnets': [
                'string',
            ]
        }
    )
    return response