"""
S3 Loader
Handles uploading data to Amazon S3
"""

import boto3
import logging
import os
from typing import Optional
from datetime import datetime
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class S3Loader:
    """Load data into Amazon S3"""
    
    def __init__(
        self, 
        bucket_name: str,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        region_name: str = 'us-east-1'
    ):
        """
        Initialize S3 loader
        
        Args:
            bucket_name: S3 bucket name
            aws_access_key_id: AWS access key (optional if using IAM roles)
            aws_secret_access_key: AWS secret key (optional if using IAM roles)
            region_name: AWS region
        """
        self.bucket_name = bucket_name
        self.region_name = region_name
        
        # Initialize S3 client
        session_kwargs = {'region_name': region_name}
        if aws_access_key_id and aws_secret_access_key:
            session_kwargs['aws_access_key_id'] = aws_access_key_id
            session_kwargs['aws_secret_access_key'] = aws_secret_access_key
        
        self.s3_client = boto3.client('s3', **session_kwargs)
        self.s3_resource = boto3.resource('s3', **session_kwargs)
    
    def create_bucket_if_not_exists(self) -> bool:
        """
        Create S3 bucket if it doesn't exist
        
        Returns:
            True if bucket was created, False if it already existed
        """
        try:
            # Check if bucket exists
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.info(f"Bucket {self.bucket_name} already exists")
            return False
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            
            if error_code == '404':
                # Bucket doesn't exist, create it
                try:
                    if self.region_name == 'us-east-1':
                        self.s3_client.create_bucket(Bucket=self.bucket_name)
                    else:
                        self.s3_client.create_bucket(
                            Bucket=self.bucket_name,
                            CreateBucketConfiguration={'LocationConstraint': self.region_name}
                        )
                    logger.info(f"Created bucket {self.bucket_name}")
                    return True
                    
                except ClientError as create_error:
                    logger.error(f"Error creating bucket: {str(create_error)}")
                    raise
            else:
                logger.error(f"Error checking bucket: {str(e)}")
                raise
    
    def upload_file(
        self, 
        local_file_path: str, 
        s3_key: str,
        metadata: Optional[dict] = None
    ) -> str:
        """
        Upload a file to S3
        
        Args:
            local_file_path: Path to local file
            s3_key: S3 object key (path in bucket)
            metadata: Optional metadata to attach to the object
            
        Returns:
            S3 URI of uploaded file
        """
        try:
            extra_args = {}
            if metadata:
                extra_args['Metadata'] = metadata
            
            self.s3_client.upload_file(
                local_file_path,
                self.bucket_name,
                s3_key,
                ExtraArgs=extra_args
            )
            
            s3_uri = f"s3://{self.bucket_name}/{s3_key}"
            logger.info(f"Uploaded {local_file_path} to {s3_uri}")
            return s3_uri
            
        except ClientError as e:
            logger.error(f"Error uploading file to S3: {str(e)}")
            raise
    
    def upload_with_date_partition(
        self,
        local_file_path: str,
        prefix: str = 'raw',
        date: Optional[datetime] = None
    ) -> str:
        """
        Upload file with date-based partitioning
        
        Args:
            local_file_path: Path to local file
            prefix: S3 prefix (folder)
            date: Date for partitioning (defaults to today)
            
        Returns:
            S3 URI of uploaded file
        """
        if date is None:
            date = datetime.utcnow()
        
        # Create date-based path: prefix/year=YYYY/month=MM/day=DD/filename
        filename = os.path.basename(local_file_path)
        s3_key = f"{prefix}/year={date.year}/month={date.month:02d}/day={date.day:02d}/{filename}"
        
        metadata = {
            'upload_timestamp': datetime.utcnow().isoformat(),
            'source_file': filename
        }
        
        return self.upload_file(local_file_path, s3_key, metadata)
    
    def list_objects(self, prefix: str = '') -> list:
        """
        List objects in S3 bucket
        
        Args:
            prefix: Filter objects by prefix
            
        Returns:
            List of object keys
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            
            if 'Contents' in response:
                objects = [obj['Key'] for obj in response['Contents']]
                logger.info(f"Found {len(objects)} objects with prefix '{prefix}'")
                return objects
            else:
                logger.info(f"No objects found with prefix '{prefix}'")
                return []
                
        except ClientError as e:
            logger.error(f"Error listing objects: {str(e)}")
            raise
    
    def download_file(self, s3_key: str, local_file_path: str) -> str:
        """
        Download file from S3
        
        Args:
            s3_key: S3 object key
            local_file_path: Local destination path
            
        Returns:
            Path to downloaded file
        """
        try:
            self.s3_client.download_file(
                self.bucket_name,
                s3_key,
                local_file_path
            )
            logger.info(f"Downloaded s3://{self.bucket_name}/{s3_key} to {local_file_path}")
            return local_file_path
            
        except ClientError as e:
            logger.error(f"Error downloading file from S3: {str(e)}")
            raise
    
    def delete_object(self, s3_key: str) -> bool:
        """
        Delete object from S3
        
        Args:
            s3_key: S3 object key
            
        Returns:
            True if successful
        """
        try:
            self.s3_client.delete_object(
                Bucket=self.bucket_name,
                Key=s3_key
            )
            logger.info(f"Deleted s3://{self.bucket_name}/{s3_key}")
            return True
            
        except ClientError as e:
            logger.error(f"Error deleting object: {str(e)}")
            raise
    
    def get_s3_uri(self, s3_key: str) -> str:
        """
        Get S3 URI for a key
        
        Args:
            s3_key: S3 object key
            
        Returns:
            S3 URI
        """
        return f"s3://{self.bucket_name}/{s3_key}"
    
    def generate_presigned_url(self, s3_key: str, expiration: int = 3600) -> str:
        """
        Generate a presigned URL for downloading an object
        
        Args:
            s3_key: S3 object key
            expiration: URL expiration time in seconds (default 1 hour)
            
        Returns:
            Presigned URL
        """
        try:
            url = self.s3_client.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': self.bucket_name,
                    'Key': s3_key
                },
                ExpiresIn=expiration
            )
            logger.info(f"Generated presigned URL for {s3_key}")
            return url
            
        except ClientError as e:
            logger.error(f"Error generating presigned URL: {str(e)}")
            raise