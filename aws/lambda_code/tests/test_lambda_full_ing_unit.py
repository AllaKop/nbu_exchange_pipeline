import aws.lambda_code.nbu_exchange_api_full_ingestion_s3
from aws.lambda_code.nbu_exchange_api_full_ingestion_s3 import build_dates, build_url, fetch_data, save_to_s3
from unittest.mock import patch, MagicMock

def test_build_dates():
    start, end = build_dates(2020, 2)
    assert start == "20200201"
    assert end == "20200229"  

def test_build_url():
    url = build_url("20200201", "20200229")
    assert "start=20200201" in url
    assert "end=20200229" in url

@patch('nbu_exchange_api_full_ingestion_S3.requests.get')
def test_fetch_data_success(mock_get):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_get.return_value = mock_response
    url = nbu_exchange_api_full_ingestion_s3.build_url("20200201", "20200202")
    response = nbu_exchange_api_full_ingestion_s3.fetch_data(url)
    assert response.status_code == 200

@patch('nbu_exchange_api_full_ingestion_S3.boto3.client')
def test_save_to_s3(mock_boto_client):
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3
    save_to_s3(mock_s3, "bucket", "key", {"a": 1})
    mock_s3.put_object.assert_called_once()