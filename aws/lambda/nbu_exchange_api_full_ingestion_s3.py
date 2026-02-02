import boto3
import requests
import json
from datetime import datetime
import calendar

def build_dates(year, month):
    start_date = f"{year}{month:02d}01"
    last_day = calendar.monthrange(year, month)[1]
    end_date = f"{year}{month:02d}{last_day:02d}"
    return start_date, end_date

def build_url(start_date, end_date):
    return f"https://bank.gov.ua/NBU_Exchange/exchange_site?start={start_date}&end={end_date}&sort=exchangedate&order=desc&json"

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
    start_year = 1996
    end_year = datetime.now().year
    errors = []

    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            start_date, end_date = build_dates(year, month)
            url = build_url(start_date, end_date)
            response = fetch_data(url)
            if response.status_code == 200:
                data = response.json()
                key = f"json/nbu_{year}{month:02d}.json"
                save_to_s3(s3, bucket, key, data)
            else:
                error_msg = f"Failed for {year}-{month:02d}: {response.status_code}"
                print(error_msg)
                errors.append(error_msg)
    return {
        'statusCode': 200,
        'body': {
            'message': 'Extraction complete',
            'errors': errors
        }
    }