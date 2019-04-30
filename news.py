from newsapi import NewsApiClient
import pprint
import boto3
from datetime import datetime
import time
from boto3.dynamodb.conditions import Key
import json


configs = [
    {
        'keyword': 'qualcomm'
    },
    {
        'keyword': 'keysights'
    },
    {
        'keyword': 'Tesla'
    },
    {
        'keyword': 'Nokia'
    },
    {
        'keyword': 'ntt docomo'
    },
    {
        'keyword': '5G'
    },
    {
        'keyword': 'Ericsson'
    }
] 

def lambda_handler(event, context):
        newsapi = NewsApiClient(api_key='87cd5ec2becb4d1784ca3a716cf9ee48')

        timestamp = (int)(time.time())
        last_timestamp = (int)(query_table())
        last_iso = datetime.utcfromtimestamp(last_timestamp).isoformat()

        

        for config in configs:
                news = newsapi.get_everything(
                                        q=config.get('keyword'),
                                        sources='bloomberg,business-insider,techcrunch-cn,techcrunch,fortune,the-economist,the-wall-street-journal',
                                        from_param=last_iso,
                                        page_size=100
                )

        for new in news.get('articles'):
                send_to_sns(new['title'] + '\n' + new['source']['name'] + '\n\n' + new['url'])

        update_table(timestamp)

# Returns last notification timestamp
def query_table():
    dynamodb = boto3.resource('dynamodb')

    table = dynamodb.Table('NewsNotification')
    response = table.query(
        KeyConditionExpression=Key('Title').eq('Time')
    )
    return response['Items'][0]['Time']

# Updates with the last notification timestamp
def update_table(timestamp):
    dynamodb = boto3.resource('dynamodb')

    table = dynamodb.Table('NewsNotification')
    table.delete_item(
        Key={
            'Title': 'Time'
        }
    )
    table.put_item(
        Item={
            'Title': 'Time',
            'Time': timestamp
        }    
    )

def send_to_sns(message):
        client = boto3.client('sns')
        client.publish(
                Message=message,
                PhoneNumber='+14803519553'
        )