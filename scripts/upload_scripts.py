#!/usr/bin/env python3
"""
Script to upload Glue scripts to S3
"""

import boto3
import os
from pathlib import Path
import argparse

def upload_scripts_to_s3(bucket_name, local_dir='src', prefix='scripts/'):
    """
    Upload all Python scripts to S3.
    
    Args:
        bucket_name: S3 bucket name
        local_dir: Local directory containing scripts
        prefix: S3 prefix for uploaded scripts
    """
    s3_client = boto3.client('s3')
    
    local_path = Path(local_dir)
    uploaded_count = 0
    
    for py_file in local_path.rglob('*.py'):
        # Get relative path
        relative_path = py_file.relative_to(local_path)
        s3_key = f"{prefix}{relative_path}"
        
        try:
            print(f"Uploading {py_file} to s3://{bucket_name}/{s3_key}")
            s3_client.upload_file(
                str(py_file),
                bucket_name,
                s3_key,
                ExtraArgs={'ServerSideEncryption': 'AES256'}
            )
            uploaded_count += 1
        except Exception as e:
            print(f"Error uploading {py_file}: {str(e)}")
    
    print(f"\nUploaded {uploaded_count} files to S3")


def main():
    parser = argparse.ArgumentParser(description='Upload Glue scripts to S3')
    parser.add_argument('--bucket', required=True, help='S3 bucket name')
    parser.add_argument('--local-dir', default='src', help='Local directory with scripts')
    parser.add_argument('--prefix', default='scripts/', help='S3 key prefix')
    
    args = parser.parse_args()
    
    upload_scripts_to_s3(args.bucket, args.local_dir, args.prefix)


if __name__ == '__main__':
    main()
