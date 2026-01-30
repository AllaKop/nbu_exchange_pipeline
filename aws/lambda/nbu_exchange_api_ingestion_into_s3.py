import json
import boto3
import requests
from datetime import datetime

def lambda_handler(event, context):
    # Set parameters
    start_date = '20260129'
    end_date = '20260129'
    currency='usd'
    url = f"https://bank.gov.ua/NBU_Exchange/exchange_site?start={start_date}&end={end_date}&valcode={currency}&sort=exchangedate&order=desc&json"
    
    # Fetch data from API
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    
    # Prepare S3 upload
    s3 = boto3.client('s3')
    bucket = 'nbu-exchange-raw'
    key = f"json/nbu_{currency}_{start_date}.json"
    
    # Upload to S3
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data),
        ContentType='application/json'
    )
    
    return {
        'statusCode': 200,
        'body': f"Data saved to s3://{bucket}/{key}"
    }