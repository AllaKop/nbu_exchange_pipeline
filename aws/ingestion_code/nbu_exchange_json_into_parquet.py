import boto3
import pandas as pd
import json
import logging
from io import BytesIO
from typing import Dict, Any, List
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    AWS Lambda handler for JSON to Parquet conversion
    
    Event structure options:
    1. Process all files: {}
    2. Process specific file: {"file_key": "json/filename.json"}
    3. Custom folders: {"source_folder": "json/", "target_folder": "parquet/"}
    """
    
    # Configuration
    BUCKET_NAME = "nbu-exchange-raw"
    DEFAULT_SOURCE_FOLDER = "json/"
    DEFAULT_TARGET_FOLDER = "parquet/"
    
    try:
        # Parse event parameters
        source_folder = event.get('source_folder', DEFAULT_SOURCE_FOLDER)
        target_folder = event.get('target_folder', DEFAULT_TARGET_FOLDER)
        specific_file = event.get('file_key')
        
        # Initialize converter
        converter = JSONToParquetConverter(BUCKET_NAME)
        
        if specific_file:
            # Convert single file
            logger.info(f"Converting single file: {specific_file}")
            success = converter.convert_file(specific_file, target_folder)
            
            return {
                'statusCode': 200 if success else 500,
                'body': json.dumps({
                    'message': f"File {specific_file} {'converted successfully' if success else 'failed to convert'}",
                    'file': specific_file,
                    'success': success
                })
            }
        else:
            # Convert all files
            logger.info(f"Converting all files from {source_folder} to {target_folder}")
            stats = converter.convert_all_files(source_folder, target_folder)
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Batch conversion completed',
                    'statistics': stats
                })
            }
            
    except Exception as e:
        logger.error(f"Lambda execution error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'message': 'Conversion failed'
            })
        }

class JSONToParquetConverter:
    def __init__(self, bucket_name: str):
        """
        Initialize the converter for Lambda environment
        Lambda automatically provides AWS credentials via IAM role
        """
        self.bucket_name = bucket_name
        self.s3_client = boto3.client('s3')
    
    def list_json_files(self, source_folder: str) -> List[str]:
        """List all JSON files in the source folder"""
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=self.bucket_name, Prefix=source_folder)
            
            json_files = []
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        if obj['Key'].endswith('.json') and obj['Size'] > 0:
                            json_files.append(obj['Key'])
            
            logger.info(f"Found {len(json_files)} JSON files in {source_folder}")
            return json_files
            
        except ClientError as e:
            logger.error(f"Error listing files: {e}")
            return []
    
    def read_json_from_s3(self, file_key: str) -> Dict[Any, Any]:
        """Read JSON file from S3"""
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=file_key)
            content = response['Body'].read().decode('utf-8')
            
            # Handle different JSON formats
            try:
                return json.loads(content)
            except json.JSONDecodeError:
                # Try to handle JSONL (JSON Lines) format
                lines = content.strip().split('\n')
                if len(lines) > 1:
                    return [json.loads(line) for line in lines if line.strip()]
                else:
                    raise
                    
        except ClientError as e:
            logger.error(f"Error reading file {file_key}: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON in file {file_key}: {e}")
            return None
    
    def convert_json_to_parquet(self, json_data: Dict[Any, Any]) -> bytes:
        """Convert JSON data to Parquet format"""
        try:
            # Handle different JSON structures
            if isinstance(json_data, list):
                if not json_data:  # Empty list
                    return None
                df = pd.DataFrame(json_data)
            elif isinstance(json_data, dict):
                df = pd.DataFrame([json_data])
            else:
                raise ValueError(f"Unsupported JSON structure: {type(json_data)}")
            
            # Handle nested objects by flattening or converting to string
            for col in df.columns:
                if df[col].dtype == 'object':
                    # Check if column contains nested objects
                    sample_val = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
                    if isinstance(sample_val, (dict, list)):
                        df[col] = df[col].apply(lambda x: json.dumps(x) if pd.notna(x) else None)
            
            # Convert to Parquet
            buffer = BytesIO()
            df.to_parquet(buffer, index=False, engine='pyarrow', compression='snappy')
            buffer.seek(0)
            return buffer.getvalue()
            
        except Exception as e:
            logger.error(f"Error converting to Parquet: {e}")
            return None
    
    def upload_parquet_to_s3(self, parquet_data: bytes, target_key: str) -> bool:
        """Upload Parquet data to S3"""
        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=target_key,
                Body=parquet_data,
                ContentType='application/octet-stream',
                Metadata={
                    'converted-from': 'json',
                    'conversion-timestamp': str(pd.Timestamp.now())
                }
            )
            logger.info(f"Successfully uploaded {target_key}")
            return True
        except ClientError as e:
            logger.error(f"Error uploading {target_key}: {e}")
            return False
    
    def convert_file(self, source_key: str, target_folder: str) -> bool:
        """Convert a single JSON file to Parquet"""
        logger.info(f"Processing file: {source_key}")
        
        # Read JSON data
        json_data = self.read_json_from_s3(source_key)
        if json_data is None:
            return False
        
        # Convert to Parquet
        parquet_data = self.convert_json_to_parquet(json_data)
        if parquet_data is None:
            return False
        
        # Generate target key
        filename = source_key.split('/')[-1].replace('.json', '.parquet')
        target_key = f"{target_folder.rstrip('/')}/{filename}"
        
        # Upload to S3
        return self.upload_parquet_to_s3(parquet_data, target_key)
    
    def convert_all_files(self, source_folder: str, target_folder: str) -> Dict[str, int]:
        """Convert all JSON files in the source folder to Parquet"""
        json_files = self.list_json_files(source_folder)
        
        if not json_files:
            logger.warning("No JSON files found to convert")
            return {"total": 0, "successful": 0, "failed": 0}
        
        successful = 0
        failed = 0
        failed_files = []
        
        for file_key in json_files:
            if self.convert_file(file_key, target_folder):
                successful += 1
            else:
                failed += 1
                failed_files.append(file_key)
        
        stats = {
            "total": len(json_files),
            "successful": successful,
            "failed": failed,
            "failed_files": failed_files
        }
        
        logger.info(f"Conversion completed: {stats}")
        return stats