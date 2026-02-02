import aws.lambda_code.nbu_exchange_api_incr_ingestion_s3
from unittest.mock import patch, MagicMock

def test_build_url():
    date = "20260202"
    url = aws.lambda_code.nbu_exchange_api_incr_ingestion_s3.build_url(date)
    assert "start=20260202" in url
    assert "end=20260202" in url

@patch('lambda_function.requests.get')
def test_fetch_data_success(mock_get):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_get.return_value = mock_response
    url = aws.lambda_code.nbu_exchange_api_incr_ingestion_s3.build_url("20260202")
    response = aws.lambda_code.nbu_exchange_api_incr_ingestion_s3.fetch_data(url)
    assert response.status_code == 200

@patch('lambda_function.boto3.client')
def test_save_to_s3(mock_boto_client):
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3
    aws.lambda_code.nbu_exchange_api_incr_ingestion_s3.save_to_s3(mock_s3, "bucket", "key", {"a": 1})
    mock_s3.put_object.assert_called_once()