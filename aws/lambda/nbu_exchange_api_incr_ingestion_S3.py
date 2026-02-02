import boto3
import requests
import json
from datetime import datetime

def build_url(date):
    return f"https://bank.gov.ua/NBU_Exchange/exchange_site?start={date}&end={date}&sort=exchangedate&order=desc&json"

def fetch_data(url):
    response = requests.get(url)
    return response

def save_to_s3(s3, bucket, key, data):
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data),
        ContentType='application/json'
    )

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    bucket = 'nbu-exchange-raw'
    date = datetime.now().strftime('%Y%m%d')
    url = build_url(date)
    errors = []

    response = fetch_data(url)
    if response.status_code == 200:
        data = response.json()
        key = f"json/nbu_{date}.json"
        save_to_s3(s3, bucket, key, data)
    else:
        error_msg = f"Failed for {date}: {response.status_code}"
        print(error_msg)
        errors.append(error_msg)
    return {
        'statusCode': 200,
        'body': {
            'message': 'Extraction complete',
            'errors': errors
        }
    }