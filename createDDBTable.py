import boto3
from dbfread import DBF
from pprint import pprint

dynamoClient = boto3.client('dynamodb')

tableCreateResponse = dynamoClient.create_table(
    AttributeDefinitions=[
        {
            'AttributeName': 'SCH_CODE',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'ROLLNO',
            'AttributeType': 'S'
        }
    ],
    TableName='SEBANET',
    KeySchema=[
        {
            'AttributeName': 'ROLLNO',
            'KeyType': 'HASH'
        },
        {
            'AttributeName': 'SCH_CODE',
            'KeyType': 'RANGE'
        }
    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 10,
        'WriteCapacityUnits': 10
    }
)
